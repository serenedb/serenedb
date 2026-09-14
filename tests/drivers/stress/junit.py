import xml.etree.ElementTree as ET


def write(path, name, seconds, findings, verdict, extra_text=""):
    suite = ET.Element("testsuite", {
        "name": "stress", "tests": "1",
        "failures": "0" if not findings and verdict == "alive" else "1",
        "errors": "0", "time": str(round(seconds, 2)),
    })
    case = ET.SubElement(suite, "testcase", {
        "name": name, "classname": "stress", "time": str(round(seconds, 2)),
    })
    if findings or verdict != "alive":
        msg = f"verdict={verdict} findings={len(findings)}"
        failure = ET.SubElement(case, "failure", {"message": msg})
        body = [msg, ""]
        for f in findings[:40]:
            body.append(f"[{f.get('kind')}] {f.get('detail')}")
        if extra_text:
            body.append("")
            body.append(extra_text)
        failure.text = "\n".join(body)
    tree = ET.ElementTree(ET.Element("testsuites"))
    tree.getroot().append(suite)
    tree.write(path, encoding="UTF-8", xml_declaration=True)
