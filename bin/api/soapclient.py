import logging as log
import requests
import untangle
from untangle import Element

def callSoap(url: str, method: str, **params) -> Element:
    methodParams = "".join(f"<{param}>{value}</{param}>" for param, value in params.items())
    body = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                         'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
                         'xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">\n'
        f'<soap12:Body><{method} xmlns="http://web.cbr.ru/">{methodParams}</{method}>\n'
        '</soap12:Body></soap12:Envelope>')

    log.debug(f"Fetching SOAP: {url}, method: {method}, params: {params}")
    headers = {"Content-Type": "text/xml; charset=utf-8"}
    response = requests.post(url, body, headers=headers)
    response.raise_for_status()

    log.debug(f"Response: {response.text}")
    xml = untangle.parse(response.text)
    return xml.soap_Envelope.soap_Body
