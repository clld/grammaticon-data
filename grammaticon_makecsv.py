#!/usr/bin/env python3

import csv
import sys
from pathlib import Path

from csvw.metadata import Column, Table, TableGroup


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
            'definition': {
                'name': 'Description',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#definition'},
            'quotation': {
                'name': 'Quotation',
                'datatype': 'string'},
            'comments': {
                'name': 'Comment',
                'datatype': 'string',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#comment'},
            'GOLD counterpart': {
                'name': 'GOLD_Counterpart',
                'datatype': 'string'},
            'GOLD URL': {
                'name': 'GOLD_URL',
                'datatype': 'string'},
            'GOLD comment': {
                'name': 'GOLD_Comment',
                'datatype': 'string'},
            'ISOCAT counterpart': {
                'name': 'ISOCAT_Counterpart',
                'datatype': 'string'},
            'ISOCAT URL': {
                'name': 'ISOCAT_URL',
                'datatype': 'string'},
            'ISOCAT comments': {
                'name': 'ISOCAT_Comments',
                'datatype': 'string'}}},

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
            # TODO: split authors?
            'authors': {
                'name': 'Authors',
                'datatype': 'string'},
            'number of features': {
                'name': 'Number_of_Features',
                'datatype': 'integer'},
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
                'name': 'Feautre_List_Numbers',
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


def simplified_concept_hierarchy(original_hierarchy, concept_ids):
    # The table looks like rows only have *either* a child_id *or* a parent id.
    # Check this assumption:
    assert all(
        # source for the hacky xor: https://stackoverflow.com/a/433161
        bool(row.get('concept_child_id')) != bool(row.get('concept_parent_id'))
        for row in original_hierarchy), 'I expect either parent or child'

    # The table also looks reflexive. Every concept--child pair seems to have
    # a redundant concept--parent pair.
    # Check this assumption:
    children = {
        (row['concept_child_id'], row['concept_id'])
        for row in original_hierarchy
        if 'concept_child_id' in row}
    parents = {
        (row['concept_id'], row['concept_parent_id'])
        for row in original_hierarchy
        if 'concept_parent_id' in row}
    assert children == parents, 'I expect all pairs to be reflexive'

    def valid_hierarchy_path(child_id, parent_id):
        if child_id not in concept_ids:
            msg = (
                'concept-hierarchy.csv:'
                " unknown child id '{}' for parent {}".format(
                    child_id, parent_id))
            print(msg, file=sys.stderr)
            return False
        elif parent_id not in concept_ids:
            msg = (
                'concept-hierarchy.csv:'
                " unknown parent id '{}' for child {}".format(
                    parent_id, child_id))
            print(msg, file=sys.stderr)
            return False
        else:
            return True

    assocs = sorted(parents, key=lambda row: tuple(map(int, row)))
    return [
        {'Child_ID': child_id, 'Parent_ID': parent_id}
        for child_id, parent_id in assocs
        if valid_hierarchy_path(child_id, parent_id)]


def main():
    here = Path(__file__).parent
    raw_dir = here / 'raw'
    csvw_dir = here / 'csvw'

    raw_tables = [
        raw_dir / 'Concepts.csv',
        raw_dir / 'Metafeatures.csv',
        raw_dir / 'Feature_lists.csv',
        raw_dir / 'Features.csv',
        raw_dir / 'Concepts_metafeatures.csv',
    ]

    table_data = {}
    # TODO: add more metadata
    # excerpt from tseltal:
    #   {"rdf:ID": "tseltal",
    #    "dc:title": "Tseltal-Spanish multidialectal dictionary",
    #    "dcat:accessURL": "https://github.com/dictionaria/tseltal",
    #    "prov:wasGeneratedBy": [
    #      {"dc:title": "python",
    #       "dc:description": "3.8.10"},
    #      {"dc:title": "python-packages",
    #       "dc:relation": "requirements.txt"}]}
    table_meta_data = TableGroup(at_props={
        "context": [
            "http://www.w3.org/ns/csvw",
            {"@language": "en"},
        ]
    })

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
            header = [
                columns[colname]['name']
                for colname in next(reader)]
            table_data[table_name] = [
                {k: v for k, v in zip(header, row) if v}
                for row in reader]

        table = Table(url=table_name)
        table.tableSchema.columns = list(
            map(Column.fromvalue, columns.values()))
        for col, target_table in table_spec.get('foreign-keys', {}).items():
            table.add_foreign_key(col, target_table, 'ID')
        table_meta_data.tables.append(table)

    # deal with the concept hierarchy separately

    with open(raw_dir / 'Concepthierarchy.csv', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = list(next(reader))
        original_hierarchy = [
            {k: v for k, v in zip(header, row) if v}
            for row in reader]

    table_data['concept-hierarchy.csv'] = simplified_concept_hierarchy(
        original_hierarchy,
        {row['ID'] for row in table_data['concepts.csv']})

    table = Table(url='concept-hierarchy.csv')
    table.tableSchema.columns = [
        Column.fromvalue({'name': 'Child_ID', 'datatype': 'string'}),
        Column.fromvalue({'name': 'Parent_ID', 'datatype': 'string'})]
    table.add_foreign_key('Child_ID', 'concepts.csv', 'ID')
    table.add_foreign_key('Parent_ID', 'concepts.csv', 'ID')
    table_meta_data.tables.append(table)

    # ensure valid data

    metafeature_ids = {row['ID'] for row in table_data['metafeatures.csv']}

    def correct_concept_metadata(row):
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
        else:
            return True

    table_data['concepts-metafeatures.csv'] = [
        row
        for row in table_data['concepts-metafeatures.csv']
        if correct_concept_metadata(row)]

    # write data

    csvw_dir.mkdir(parents=True, exist_ok=True)
    table_meta_data.write(csvw_dir / 'csvw-metadata.json', **table_data)


if __name__ == '__main__':
    main()
