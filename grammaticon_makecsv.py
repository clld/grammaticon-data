#!/usr/bin/env python3

import csv
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path

from csvw.metadata import Column, Table, TableGroup
from simplepybtex.database import BibliographyData, parse_file

RAW_TO_CSWV_MAP = {
    'Concepts.csv': {
        'name': 'concepts.csv',
        'columns': {
            'id': {
                'name': 'ID',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id'},
            'label': {
                'name': 'Name',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#name'},
            'grammacode': {
                'name': 'Grammacode',
                'datatype': 'string'},
            'definition': {
                'name': 'Description',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#definition'},
            'comments': {
                'name': 'Comment',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#comment'},
            'SIL counterpart': {
                'name': 'SIL_Counterpart',
                'datatype': 'string'},
            'SIL URL': {
                'name': 'SIL_URL',
                'datatype': 'string'},
            'Wikipedia counterpart': {
                'name': 'Wikipedia_Counterpart',
                'datatype': 'string'},
            'Wikipedia URL': {
                'name': 'Wikipedia_URL',
                'datatype': 'string'},
            'Croft counterpart': {
                'name': 'Croft_counterpart',
                'datatype': 'string'},
            'Croft definition': {
                'name': 'Croft_definition',
                'datatype': 'string'},
            'Croft URL': {
                'name': 'Croft_URL',
                'datatype': 'string'},
            'GOLD counterpart': {
                'name': 'GOLD_counterpart',
                'datatype': 'string'},
            'ISOCAT counterpart': {
                'name': 'ISOCAT_counterpart',
                'datatype': 'string'},
            'quotation': {
                'name': 'Quotation',
                'datatype': 'string'},
            'Bibsources': {
                'name': 'Source',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#source',
                'separator': ';'}}},

    'Metafeatures.csv': {
        'name': 'metafeatures.csv',
        'columns': {
            'id': {
                'name': 'ID',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id'},
            'name': {
                'name': 'Name',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#name'},
            'feature_area': {
                'name': 'Feature_Area',
                'datatype': 'string'}}},

    'Feature_lists.csv': {
        'name': 'feature-lists.csv',
        'columns': {
            'id': {
                'name': 'ID',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id'},
            'name': {
                'name': 'Name',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#name'},
            'URL': {
                'name': 'URL',
                'datatype': 'string'},
            'description': {
                'name': 'Description',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#description'},
            'year': {
                'name': 'Year',
                'datatype': 'string'}}},

    'Features.csv': {
        'name': 'features.csv',
        'columns': {
            'feature_ID': {
                'name': 'ID',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id'},
            'feature name': {
                'name': 'Name',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#name'},
            'feature description': {
                'name': 'Description',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#definition'},
            'meta_feature_id': {
                'name': 'Metafeature_ID',
                'datatype': 'string'},
            'collection_id': {
                'name': 'Feature_List_ID',
                'datatype': 'string'},
            'collection URL': {
                'name': 'Feature_List_URL',
                'datatype': 'string'},
            # TODO: should this be a list?
            'collection numbers': {
                'name': 'Feature_List_Numbers',
                'datatype': 'string'}},
        'foreign-keys': {
            'Metafeature_ID': 'metafeatures.csv',
            'Feature_List_ID': 'feature-lists.csv'}},

    'Concepts_metafeatures.csv': {
        'name': 'concepts-metafeatures.csv',
        'columns': {
            'concept_id': {
                'name': 'Concept_ID',
                'datatype': 'string'},
            'meta_feature__id': {
                'name': 'Metafeature_ID',
                'datatype': 'string'}},
        'foreign-keys': {
            'Concept_ID': 'concepts.csv',
            'Metafeature_ID': 'metafeatures.csv'}}}


CONCEPT_ID_COL = 'concept_id'
CHILD_COL = 'concept_child_id'
PARENT_COL = 'concept_parent_id'

BIBKEY_FIXES = {
    'blomfield_language_1933': 'bloomfield_language_1933',
    'croft_morphosyntax_ 2022': 'croft_morphosyntax_2022',
}


def simplified_concept_hierarchy(original_hierarchy, concept_ids):
    # The table looks like rows only have *either* a child_id *or* a parent id.
    # Check this assumption:
    conflicting = [
        row
        for row in original_hierarchy
        # source for the hacky xor: https://stackoverflow.com/a/433161
        if bool(row.get(CHILD_COL)) == bool(row.get(PARENT_COL))]
    assert not conflicting, f'concepthierarchy: concepts with parent *and* child: {conflicting}'

    # The table also looks reflexive. Every concept--child pair seems to have
    # a redundant concept--parent pair.
    # Check this assumption:
    children = {
        (row[CHILD_COL], row[CONCEPT_ID_COL])
        for row in original_hierarchy
        if row.get(CHILD_COL) in concept_ids
        and row.get(CONCEPT_ID_COL) in concept_ids}
    parents = {
        (row[CONCEPT_ID_COL], row[PARENT_COL])
        for row in original_hierarchy
        if row.get(PARENT_COL) in concept_ids
        and row.get(CONCEPT_ID_COL) in concept_ids}
    assert children == parents, 'I expect all pairs to be reflexive'

    def valid_hierarchy_path(child_id, parent_id):
        if child_id not in concept_ids:
            msg = (
                'concept-hierarchy.csv:'
                f" unknown child id '{child_id}' for parent {parent_id}")
            print(msg, file=sys.stderr)
            return False
        elif parent_id not in concept_ids:
            msg = (
                'concept-hierarchy.csv:'
                f" unknown parent id '{parent_id}' for child {child_id}")
            print(msg, file=sys.stderr)
            return False
        else:
            return True

    assocs = sorted(parents, key=lambda row: tuple(map(int, row)))
    return [
        {'Child_ID': child_id, 'Parent_ID': parent_id}
        for child_id, parent_id in assocs
        if valid_hierarchy_path(child_id, parent_id)]


def is_concept_valid(row):
    if 'Name' not in row:
        msg = (
            'concepts.csv:'
            ' missing name for concept {}'.format(row['ID']))
        print(msg, file=sys.stderr)
        return False
    else:
        return True


def is_concept_metafeature_valid(row, concept_ids, metafeature_ids):
    if 'Metafeature_ID' not in row:
        msg = (
            'concepts-metafeatures.csv:'
            ' missing metafeature id for concept {}'.format(
                row['Concept_ID']))
        print(msg, file=sys.stderr)
        return False
    elif (mfid := row['Metafeature_ID']) not in metafeature_ids:
        msg = (
            'concepts-metafeatures.csv:'
            ' invalid metafeature id for concept {}: {}'.format(
                row['Concept_ID'], mfid))
        print(msg, file=sys.stderr)
        return False
    elif (cid := row['Concept_ID']) not in concept_ids:
        msg = (
            'concepts-metafeatures.csv:'
            ' invalid concept id for metafeature {}: {}'.format(
                row['Metafeature_ID'], cid))
        print(msg, file=sys.stderr)
        return False
    else:
        return True


def is_feature_valid(row, metafeature_ids, feature_list_ids):
    if 'Metafeature_ID' not in row or 'Feature_List_ID' not in row:
        msg = (
            'features.csv:'
            ' missing metafeature id or missing feature list id'
            ' for feature {}'.format(row['ID']))
        print(msg, file=sys.stderr)
        return False
    elif (mfid := row['Metafeature_ID']) not in metafeature_ids:
        msg = (
            'features.csv:'
            ' invalid metafeature id for feature {}: {}'.format(
                mfid, row['ID']))
        print(msg, file=sys.stderr)
        return False
    elif (flid := row['Feature_List_ID']) not in feature_list_ids:
        msg = (
            'features.csv:'
            ' invalid feature list id for feature {}: {}'.format(
                flid, row['ID']))
        print(msg, file=sys.stderr)
        return False
    else:
        return True


def main():
    here = Path(__file__).parent
    raw_dir = here / 'raw'
    csv_dir = raw_dir / 'csv-export'
    dest_dir = here / 'csvw'

    raw_tables = [
        csv_dir / 'Concepts.csv',
        csv_dir / 'Metafeatures.csv',
        csv_dir / 'Feature_lists.csv',
        csv_dir / 'Features.csv',
        csv_dir / 'Concepts_metafeatures.csv',
    ]

    table_props = {
        'rdf:ID': 'grammaticon',
        'dc:title': 'Grammaticon',
        'dc:source': 'sources.bib',
        'dcat:accessURL': 'https://github.com/clld/grammaticon-data',
        'prov:wasGeneratedBy': [
            {'dc:title': 'python',
             'dc:description': platform.python_version()},
             # TODO: do a pip freeze on the venv
             # {'dc:title': 'python-packages',
             #  'dc:relation': 'requirements.txt'},
        ],
    }

    if here.joinpath('.git').is_dir():
        git_remote = None
        with open(here / '.git' / 'config', encoding='utf-8') as f:
            for line in f:
                if re.fullmatch(r'\s*\[\s*remote\s+"origin"\s*\]\s*', line):
                    break
            for line in f:
                if (m := re.fullmatch(r'\s*url\s*=\s*(\S+)\s*', line)):
                    git_remote = m.group(1)
                    break
                elif re.match(r'\s*\[', line):
                    break
        if git_remote:
            git_exe = shutil.which('git')
            assert git_exe is not None
            procresult = subprocess.run(
                [git_exe, '-C', str(here), 'describe', '--always', '--tags'],
                stdout=subprocess.PIPE, check=True, encoding='utf-8')
            git_description = procresult.stdout.strip()
            table_props["prov:wasDerivedFrom"] = [
                {
                    'dc:title': 'Repository',
                    'rdf:type': 'prov:Entity',
                    'rdf:about': git_remote,
                    'dc:created': git_description,
                },
            ]

    table_data = {}
    table_meta_data = TableGroup(
        at_props={
            "context": [
                "http://www.w3.org/ns/csvw",
                {"@language": "en"},
            ],
        },
        common_props=table_props,
    )

    # load data

    # FIXME: meta feature names are not unique
    # So far the webapp has just collapsed them and mapped all the references
    # in other tables to the first occurence:
    #
    #     | ID | Name   | Area         |   | Bla | Metafeature_ID |
    #     |----+--------+--------------|   |-----+----------------|
    #     |  1 | name 1 | an area      |   | ... |              1 |
    #     |  2 | name 2 | another area |   | ... |              2 |
    #     |  3 | name 1 | an area      |   | ... |              3 |
    #     |  4 | name 4 |              |   | ... |              4 |
    #
    #                 vvvv                           vvvv
    #
    #     | ID | Name   | Area         |   | Bla | Metafeature_ID |
    #     |----+--------+--------------|   |-----+----------------|
    #     |  1 | name 1 | an area      |   | ... |              1 |
    #     |  2 | name 2 | another area |   | ... |              2 |
    #     |  4 | name 4 |              |   | ... |              1 |
    #                                      | ... |              4 |
    #
    # It does not do that anymore because this should be handled at the data
    # curation level.
    for raw_path in raw_tables:
        table_spec = RAW_TO_CSWV_MAP[raw_path.name]
        table_name = table_spec['name']
        columns = table_spec['columns']
        with open(raw_path, encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            missing = [col for col in columns if col not in header]
            assert not missing, f'{raw_path}: missing fields: {missing}'
            unknown_fields = [col for col in header if col not in columns]
            assert not unknown_fields, f'{raw_path}: unknown fields: {unknown_fields}'
            mapped_colnames = [
                columns[colname]['name']
                for colname in header]
            table_data[table_name] = [
                {k: v for k, v in zip(mapped_colnames, row) if v}
                for row in reader]

        table = Table(url=table_name)
        table.tableSchema.columns = list(
            map(Column.fromvalue, columns.values()))
        for col, target_table in table_spec.get('foreign-keys', {}).items():
            table.add_foreign_key(col, target_table, 'ID')
        table_meta_data.tables.append(table)

    # deal with the concept hierarchy separately

    with open(csv_dir / 'Concepthierarchy.csv', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = list(next(reader))
        original_hierarchy = [
            {k: v for k, v in zip(header, row) if v}
            for row in reader]

    sources = parse_file(raw_dir / 'sources.bib')

    # split the references
    for concept in table_data['concepts.csv']:
        if (source := concept.get('Source')):
            concept['Source'] = re.split(r'\s*;\s*', source)

    concept_ids = {row['ID'] for row in table_data['concepts.csv']}
    table_data['concept-hierarchy.csv'] = simplified_concept_hierarchy(
        original_hierarchy, concept_ids)

    table = Table(url='concept-hierarchy.csv')
    table.tableSchema.columns = [
        Column.fromvalue({'name': 'Child_ID', 'datatype': 'string'}),
        Column.fromvalue({'name': 'Parent_ID', 'datatype': 'string'})]
    table.add_foreign_key('Child_ID', 'concepts.csv', 'ID')
    table.add_foreign_key('Parent_ID', 'concepts.csv', 'ID')
    table_meta_data.tables.append(table)

    # ensure valid data

    metafeature_ids = {row['ID'] for row in table_data['metafeatures.csv']}
    assert '' not in metafeature_ids
    assert None not in metafeature_ids
    feature_list_ids = {r['ID'] for r in table_data['feature-lists.csv']}
    assert '' not in feature_list_ids
    assert None not in feature_list_ids

    for row in table_data['concepts.csv']:
        if (refs := row.get('Source')):
            row['Source'] = [BIBKEY_FIXES.get(key) or key for key in refs]

    bibkeys = {
        re.fullmatch(r'([^[]+)(?:\[[^\]]*\])?', citation).group(1).lower()
        for row in table_data['concepts.csv']
        for citation in row.get('Source') or ()}
    missing_bibkeys = {
        bibkey
        for bibkey in bibkeys
        if bibkey not in sources.entries}
    if missing_bibkeys:
        print('\n'.join(
            f'bibkey not found in bibliography: {bibkey}'
            for bibkey in sorted(missing_bibkeys)))

    sources = BibliographyData(
        entries=sources.entries.__class__(
            (k, b)
            for k, b in sources.entries.items()
            if k.lower() in bibkeys))

    table_data['concepts.csv'] = [
        row
        for row in table_data['concepts.csv']
        if is_concept_valid(row)]
    table_data['concepts-metafeatures.csv'] = [
        row
        for row in table_data['concepts-metafeatures.csv']
        if is_concept_metafeature_valid(row, concept_ids, metafeature_ids)]
    table_data['features.csv'] = [
        row
        for row in table_data['features.csv']
        if is_feature_valid(row, metafeature_ids, feature_list_ids)]

    # write data

    dest_dir.mkdir(parents=True, exist_ok=True)
    table_meta_data.write(dest_dir / 'csvw-metadata.json', **table_data)
    sources.to_file(str(dest_dir / 'sources.bib'), 'bibtex')


if __name__ == '__main__':
    main()
