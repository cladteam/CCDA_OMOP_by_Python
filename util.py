
import time
import vocab_map_file

def convert_date(hl7_date):
    date_struct = time.strptime(hl7_date, '%Y%m%d')
    omop_date = time.strftime('%Y-%m-%d', date_struct)

    return omop_date


def check_CCD_document_type(tree):
    child_list = tree.findall(".")
    child = child_list[0]

    gen_id = child.findall("./{urn:hl7-org:v3}templateId[@root='" + vocab_map_file.US_general_root + "']")[0].attrib['root']
    ccd_id = child.findall("./{urn:hl7-org:v3}templateId[@root='" + vocab_map_file.ccd_document_root + "']")[0].attrib['root']
    return not (gen_id is None or ccd_id is None )

