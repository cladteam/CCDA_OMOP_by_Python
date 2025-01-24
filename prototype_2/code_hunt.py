#!/usr/bin/env python3

# code_hunt.py
#
# Use the infrastrcuture here to find one or more mappings for a vocabulary/codeSystem and code pair
# and identify which mapping tables were used.

import argparse
import prototype_2.value_transformations as VT


def main() :
    prefix="2.16.840.1.113883."
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    parser.add_argument('-v', '--vocabulary', help=f"vocabulary OID / codeSystem has {prefix}  prefix")
    parser.add_argument('-c', '--code', help="concept code")
    args = parser.parse_args()

    vocab = prefix + args.vocabulary
    print(f"Codemap {vocab} {args.code}" )
    concept_id = VT._codemap_xwalk(vocab, args.code, 'target_concept_id', 0)
    domain_id = VT._codemap_xwalk(vocab, args.code, 'target_domain_id', 0)
    source_concept_id = VT._codemap_xwalk(vocab, args.code, 'source_concept_id', 0)
    print(f"codemap concept_id:{concept_id} domain:{domain_id}   source concept_id:{source_concept_id}")
    print("")
    
    print(f"Visit xwalk {vocab} {args.code}")
    concept_id = VT._visit_xwalk(vocab, args.code, 'target_concept_id', 0)
    domain_id = VT._visit_xwalk(vocab, args.code, 'target_domain_id', 0)
    print(f"visit map concept_id:{concept_id} domain_id:{domain_id}")
    print("")
    
    print(f"Valueset xwalk {vocab} {args.code}")
    concept_id = VT._valueset_xwalk(vocab, args.code, 'target_concept_id', 0)
    domain_id = VT._valueset_xwalk(vocab, args.code, 'target_domain_id', 0)
    print(f"valueset map concept_id:{concept_id} domain_id:{domain_id}")
    print("")

if __name__ == '__main__':
    main()

