

""" misc. utilities """

import time
import vocab_map_file
from xml_ns import ns


def convert_date(hl7_date):
    """
        converts an HL7 formatted date (without dashes) to an OMOP or
        Postgres formatted one that has dashes.
    """
    date_struct = time.strptime(hl7_date, '%Y%m%d')
    omop_date = time.strftime('%Y-%m-%d', date_struct)

    return omop_date


def check_ccd_document_type(tree):
    """ gets two document-level template IDs and compres them to
        expectations for a CCD document.
    """
    child_list = tree.findall(".", ns)
    child = child_list[0]

    try:
        gen_id = child.findall("./templateId[@root='" +
                               vocab_map_file.US_GENERAL_ROOT + "']", ns)[0].\
            attrib['root']
        ccd_id = child.findall("./templateId[@root='" +
                               vocab_map_file.CCD_DOCUMENT_ROOT + "']", ns)[0].\
            attrib['root']
        return not (gen_id is None or ccd_id is None)
    except IndexError:
        return False

    return False
