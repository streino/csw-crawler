import csv
import logging
import re
import unicodedata

from dataclasses import dataclass
from pathlib import Path

import scrapy

from lxml import etree
from scrapy.spidermiddlewares.httperror import HttpError


SPIDER_SETTINGS = {
    "CONCURRENT_REQUESTS": 100,
    "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    "DOWNLOAD_DELAY": 0.5,
    "DOWNLOAD_TIMEOUT": 60,
    "LOG_LEVEL": logging.INFO,
    "RETRY_TIMES": 1,
    "ROBOTSTXT_OBEY": False,
}

DEFAULT_OUTPUT_DIR = "output"

OUTPUT_SCHEMA = "http://www.isotc211.org/2005/gmd"
PAGE_SIZE = 10

NS = {
    "csw": "http://www.opengis.net/cat/csw/2.0.2",
    "gco": "http://www.isotc211.org/2005/gco",
    "gmd": "http://www.isotc211.org/2005/gmd",
}


def _slugify(string: str) -> str:
    t = unicodedata.normalize("NFKD", string).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[\W_]+", "-", t).strip("-")[:64] or "unknown"


def _csw_request(start: int) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecords
    xmlns:apiso="http://www.opengis.net/cat/csw/apiso/1.0"
    xmlns:csw="http://www.opengis.net/cat/csw/2.0.2"
    xmlns:gmd="http://www.isotc211.org/2005/gmd"
    xmlns:ogc="http://www.opengis.net/ogc"
    service="CSW" version="2.0.2"
    resultType="results"
    startPosition="{start}"
    maxRecords="{PAGE_SIZE}"
    outputFormat="application/xml"
    outputSchema="{OUTPUT_SCHEMA}">
  <csw:Query typeNames="gmd:MD_Metadata">
    <csw:ElementSetName>full</csw:ElementSetName>
    <csw:Constraint version="1.1.0">
      <ogc:Filter>
        <ogc:Or>
          <ogc:PropertyIsEqualTo>
            <ogc:PropertyName>apiso:type</ogc:PropertyName>
            <ogc:Literal>dataset</ogc:Literal>
          </ogc:PropertyIsEqualTo>
          <ogc:PropertyIsEqualTo>
            <ogc:PropertyName>apiso:type</ogc:PropertyName>
            <ogc:Literal>nonGeographicDataset</ogc:Literal>
          </ogc:PropertyIsEqualTo>
          <ogc:PropertyIsEqualTo>
            <ogc:PropertyName>apiso:type</ogc:PropertyName>
            <ogc:Literal>service</ogc:Literal>
          </ogc:PropertyIsEqualTo>
          <ogc:PropertyIsEqualTo>
            <ogc:PropertyName>apiso:type</ogc:PropertyName>
            <ogc:Literal>series</ogc:Literal>
          </ogc:PropertyIsEqualTo>
        </ogc:Or>
      </ogc:Filter>
    </csw:Constraint>
    <ogc:SortBy>
      <ogc:SortProperty>
        <ogc:PropertyName>apiso:identifier</ogc:PropertyName>
        <ogc:SortOrder>ASC</ogc:SortOrder>
      </ogc:SortProperty>
    </ogc:SortBy>
  </csw:Query>
</csw:GetRecords>"""


@dataclass
class Endpoint:
    label: str
    url: str

    def __init__(self, id: str, name: str, url: str):
        self.label = _slugify(name) + "--" + id
        self.url = url


class CswSpider(scrapy.Spider):
    name = "csw-spider"
    custom_settings = SPIDER_SETTINGS


    def __init__(self, endpoints: str = "", *args, **kwargs):
        super().__init__(*args, **kwargs)

        with Path(endpoints).open(mode="r", newline="") as fin:
            reader = csv.DictReader(fin, delimiter=";")
            self.endpoints = [
                Endpoint(row["id"], row["name"], row["url"])
                for row in reader
                if self._accept(row)
            ]
        if not self.endpoints:
            raise ValueError("Provide URLs via -a endpoints=path")

        self.output_dir = Path(getattr(self, "OUTPUT_DIR", DEFAULT_OUTPUT_DIR))


    async def start(self):
        for endpoint in self.endpoints:
            self.logger.info(f"Crawling {endpoint.url} for {endpoint.label}")
            yield self._make_request(endpoint)


    def parse(self, response):
        endpoint = response.meta["endpoint"]
        start = response.meta["start"]

        try:
            root = etree.fromstring(response.body)
        except etree.XMLSyntaxError as exc:
            self.logger.error(f"{endpoint.url}[start={start}]: {exc}")
            return

        results = root.find(".//csw:SearchResults", NS)
        if results is None:
            self.logger.warning(f"{endpoint.url}[start={start}]: no SearchResults in response")
            return

        total = int(results.get("numberOfRecordsMatched", 0))
        next = int(results.get("nextRecord", 0))
        self.logger.info(f"{endpoint.url}[start={start}]: total={total} next={next}")

        dest = self.output_dir / endpoint.label
        dest.mkdir(parents=True, exist_ok=True)

        for i, record in enumerate(results):
            r = record.xpath(".//gmd:fileIdentifier/gco:CharacterString", namespaces=NS)
            id = (r[0].text or "").strip() if r else None
            if not id:
                self.logger.warning(f"{endpoint.url}[start={start}]: record {i}-{start+i} has no fielIdentifier")
                continue
            filepath = dest / f"{_slugify(id)}.xml"
            filepath.write_bytes(
                etree.tostring(record, pretty_print=True, xml_declaration=True, encoding="UTF-8")
            )

        if 0 < next <= total:
            yield self._make_request(endpoint, start=next)


    def _accept(self, row) -> bool:
        if row.get("backend") in ("csw-iso-19139", None):
            return True
        if row.get("validation") in ("accepted", None):
            return True
        return False


    def _make_request(self, endpoint: Endpoint, start: int = 1) -> scrapy.Request:
        self.logger.debug(f"Queueing {endpoint.url}[start={start}] for {endpoint.label}")

        return scrapy.Request(
            endpoint.url,
            method="POST",
            headers={"Accept": "application/xml", "Content-Type": "application/xml"},
            body=_csw_request(start),
            errback=self._errback,
            meta={"endpoint": endpoint, "start": start},
            dont_filter=True,
        )


    def _errback(self, failure):
        self.logger.error(f"Failed request: {failure.request.url} - {failure.value}")

        # GeoIDE bad items fix
        if failure.check(HttpError) and failure.value.response.status == 504:
            meta = failure.value.response
            endpoint = meta["endpoint"]
            start = meta["start"]
            next = start + 1
            self.logger.info(f"{failure.request.url}[start={start}]: retry from next item")
            yield self._make_request(endpoint, start=next)
