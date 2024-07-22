from util.xml_ns import ns
from util.vocab_map_file import oid_map

def show_effective_time(entity):
    # effectiveTime
    for time_element in entity.findall('./effectiveTime', ns):
        if 'value' in time_element.attrib:
            print(f" time:{time_element.attrib['value']}", end="")
        else:
            for part in time_element.findall('./*', ns):
                if 'value' in time_element.attrib:
                    print(f" time:{time_element.attrib['value']}", end="")


def show_id(entity):
    # ID : root, translation
    for id_element in entity.findall('./id', ns):
        if 'root' in id_element:
            print(f" root:{id_element.attrib['root']}", end="")
        if 'translation' in id_element:
            print(f" translation:{id_element.attrib['translation']},", end=' ')


def show_code(entity):
    # CODE : code, displayName, codeSystem
    for code_element in entity.findall('./code', ns):
        if 'codeSystem' in code_element.attrib:
            vocabulary_id = oid_map[code_element.attrib['codeSystem']][0]
            display_name = ''
            code = ''
            if 'displayName' in code_element.attrib:
                display_name = code_element.attrib['displayName']
            if 'code' in code_element.attrib:
                code = code_element.attrib['code']
            print(f" code: {display_name}, {vocabulary_id}, {code}, ", end=' ')
        else:
            print(" code: 'N/A' ", end=' ')


def show_value(entity):
    # VALUE : value, unit, type
    value_string = "value: "
    for value_element in entity.findall('./value', ns):
        if 'value' in value_element.attrib:
            value_string = f"{value_string}, {value_element.attrib['value']} "
        else:
            value_string = f"{value_string}, None "
        if 'unit' in value_element.attrib:
            value_string = f"{value_string}, {value_element.attrib['unit']}  "
        else:
            value_string = f"{value_string}, None "
        if 'xsi:type' in value_element.attrib:
            value_string = f"{value_string}, {value_element.attrib['xsi:type']} "
        else:
            value_string = f"{value_string}, None "
    print(value_string, end='')

# def show_person(entity):

# def show_performer(entity):

# def show_organization(entity):

# def show_encounter(entity):
