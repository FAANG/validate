import requests
from datetime import datetime
from metadata_validation_conversion.constants import \
    SAMPLES_ALLOWED_SPECIAL_SHEET_NAMES
from validation.helpers import get_record_name
from submission.helpers import remove_underscores


class BiosamplesFileConverter:
    def __init__(self, json_to_convert):
        self.json_to_convert = json_to_convert

    def start_conversion(self):
        data_to_send = list()
        taxon_ids, taxons = self.get_taxon_information()
        current_date = datetime.now().strftime("%Y-%m-%d")
        # Collect additional data
        additional_fields = dict()
        for additional_field in SAMPLES_ALLOWED_SPECIAL_SHEET_NAMES:
            for item in self.json_to_convert[additional_field]:
                for existing_field, existing_value in item.items():
                    additional_fields.setdefault(
                        remove_underscores(existing_field), list())
                    additional_fields[remove_underscores(
                        existing_field)].append({'value': existing_value})
        for record_type, records in self.json_to_convert.items():
            if record_type in SAMPLES_ALLOWED_SPECIAL_SHEET_NAMES:
                continue
            for record_index, record in enumerate(records):
                record_name = get_record_name(record, record_index, record_type)
                data_to_send.append(
                    {
                        "alias": record_name,
                        "title": record_name,
                        "releaseDate": current_date,
                        "taxonId": taxon_ids[record_name],
                        "taxon": taxons[record_name],
                        "attributes": self.get_sample_attributes(
                            record, additional_fields),
                        "sampleRelationships": self.get_sample_relationships(
                            record)
                    }
                )
        return data_to_send

    def get_taxon_information(self):
        """
        This function will parse whole json to get all taxon ids
        :return: dict with id as key and taxon id as value
        """
        taxon_ids = dict()
        taxons = dict()
        missing_ids = dict()
        for record_type, records in self.json_to_convert.items():
            if record_type in SAMPLES_ALLOWED_SPECIAL_SHEET_NAMES:
                continue
            for record_index, record in enumerate(records):
                record_name = get_record_name(record, record_index, record_type)
                if 'organism' in record:
                    taxon_ids[record_name] = \
                        record['organism']['term'].split(":")[1]
                    taxons[record_name] = record['organism']['text']
                elif 'derived_from' in record:
                    if isinstance(record['derived_from'], dict):
                        missing_ids[record_name] = \
                            record['derived_from']['value']
                    elif isinstance(record['derived_from'], list):
                        missing_ids[record_name] = \
                            record['derived_from'][0]['value']
        for id_to_fetch in missing_ids:
            taxon_ids[id_to_fetch], taxons[id_to_fetch] = \
                self.fetch_taxon_information(id_to_fetch, taxon_ids,
                                             taxons, missing_ids)
        return taxon_ids, taxons

    def fetch_taxon_information(self, id_to_fetch, taxon_ids, taxons,
                                missing_ids):
        """
        This function will find taxon id for particular record
        :param id_to_fetch: id to check
        :param taxon_ids: existing taxon ids
        :param taxons: existing taxons
        :param missing_ids: missing taxon ids
        :return:
        """
        if id_to_fetch in taxon_ids and id_to_fetch in taxons:
            return taxon_ids[id_to_fetch], taxons[id_to_fetch]
        else:
            # TODO: return error in taxon is not in biosamples
            if 'SAM' in id_to_fetch:
                try:
                    results = requests.get(
                        f"https://www.ebi.ac.uk/biosamples/samples/"
                        f"{id_to_fetch}").json()
                    return results['taxId'], results['characteristics'][
                        'organism'][0]['text']
                except ValueError:
                    pass
            else:
                return self.fetch_taxon_information(missing_ids[id_to_fetch],
                                                    taxon_ids,
                                                    taxons,
                                                    missing_ids)

    @staticmethod
    def get_sample_relationships(record):
        """
        This function will parse record and find all relationships
        :param record: record to parse
        :return: list of all relationships
        """
        sample_relationships = list()
        if 'same_as' in record:
            sample_relationships.append(
                {
                    "alias": record['same_as']['value'],
                    "relationshipNature": "same as"
                }
            )
        if 'child_of' in record:
            for child in record['child_of']:
                sample_relationships.append(
                    {
                        "alias": child['value'],
                        "relationshipNature": "child of"
                    }
                )
        if 'derived_from' in record:
            if isinstance(record['derived_from'], list):
                for child in record['derived_from']:
                    sample_relationships.append(
                        {
                            "alias": child['value'],
                            "relationshipNature": "derived from"
                        }
                    )
            elif isinstance(record['derived_from'], dict):
                sample_relationships.append(
                    {
                        "alias": record['derived_from']['value'],
                        "relationshipNature": "derived from"
                    }
                )
        return sample_relationships

    def get_sample_attributes(self, record, additional_fields):
        """
        This function will return biosample attributes
        :param record: record to parse
        :param additional_fields: additional fields to add to dict
        :return: attributes for this record in biosample format
        """
        sample_attributes = dict()
        for attribute_name, attribute_value in record.items():
            if attribute_name == 'samples_core':
                for sc_attribute_name, sc_attribute_value in \
                        record['samples_core'].items():
                    sample_attributes[remove_underscores(sc_attribute_name)] = \
                        self.parse_attribute(sc_attribute_value)
            elif attribute_name == 'custom':
                for sc_attribute_name, sc_attribute_value in \
                        record['custom'].items():
                    sample_attributes[remove_underscores(sc_attribute_name)] = \
                        self.parse_attribute(sc_attribute_value)
            else:
                sample_attributes[remove_underscores(attribute_name)] = \
                    self.parse_attribute(attribute_value)
        sample_attributes.update(additional_fields)
        return sample_attributes

    def parse_attribute(self, value_to_parse):
        """
        This function will parse single attribute from data
        :param value_to_parse: data to parse
        :return: attributes list
        """
        attributes = list()

        if isinstance(value_to_parse, list):
            for sc_value_to_parse in value_to_parse:
                attributes.append(
                    self.parse_value_in_attribute(sc_value_to_parse)
                )
        elif isinstance(value_to_parse, dict):
            attributes.append(
                self.parse_value_in_attribute(value_to_parse)
            )
        return attributes

    @staticmethod
    def parse_value_in_attribute(value_to_parse):
        """
        This function will parse single fields
        :param value_to_parse: field to parse
        :return: dict of attributes
        """
        attribute = dict()
        if 'text' in value_to_parse:
            attribute['value'] = value_to_parse['text']
        elif 'value' in value_to_parse:
            attribute['value'] = value_to_parse['value']

        if 'term' in value_to_parse:
            ontology_url = "_".join(value_to_parse['term'].split(":"))
            attribute['terms'] = [
                {
                    "url": f"http://purl.obolibrary.org/obo/{ontology_url}"
                }
            ]

        if 'units' in value_to_parse:
            attribute['units'] = value_to_parse['units']
        return attribute
