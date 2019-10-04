import requests
from metadata_validation_conversion.helpers import get_samples_json
from metadata_validation_conversion.constants import SKIP_PROPERTIES


def validate(data, schema):
    """
    This function will send data to elixir-validator and collect all errors
    :param data: data to validate in JSON format
    :param schema: schema to validate against
    :return: list of error messages
    """
    json_to_send = {
        'schema': schema,
        'object': data
    }
    response = requests.post(
        'http://localhost:3020/validate', json=json_to_send).json()
    validation_errors = list()
    if 'validationErrors' in response and len(response['validationErrors']) > 0:
        for error in response['validationErrors']:
            validation_errors.append(error['userFriendlyMessage'])
    return validation_errors


def collect_recommended_fields(json_to_check):
    """
    This function will return list of recommended fields
    :param json_to_check: json to check for recommended fields
    :return: list with recommended fields
    """
    recommended_fields = list()
    for field_name, field_value in json_to_check['properties'].items():
        if field_name not in SKIP_PROPERTIES:
            if field_value['type'] == 'object':
                if field_value['properties']['mandatory']['const'] == \
                        'recommended':
                    recommended_fields.append(field_name)
            elif field_value['type'] == 'array':
                if field_value['items']['properties']['mandatory']['const'] == \
                        'recommended':
                    recommended_fields.append(field_name)
    return recommended_fields


def check_item_is_present(dict_to_check, list_of_items):
    """
    This function will collect all field names that are not in data
    :param dict_to_check: data to check
    :param list_of_items: list of items to search in data
    :return: list of items that are not in data
    """
    warnings = list()
    for item in list_of_items:
        if item not in dict_to_check:
            warnings.append(item)
    return warnings


def check_ontology_text(record, ontology_names):
    for field_name, field_value in record.items():
        if 'text' in field_value and 'term' in field_value:
            term_labels = requests.get(
                f"http://www.ebi.ac.uk/ols/api/terms?id={field_value['term']}"
            ).json()['_embedded']['terms']
            term_label = ''
            for label in term_labels:
                if label['ontology_name'].lower() == ontology_names[field_name].lower():
                    term_label = label['label']
            print(f"{term_label}\t{field_value['text']}")
            # if term_label == '':
            #     print(f"Error!")
            # if field_value['text'].lower() != term_label:
            #     print(f"Provided value '{field_value['text']}' doesn't precisely match label '{term_label}' for term '{field_value['term']}'")


def check_date_units():
    pass


def check_breeds():
    pass


def check_relationships():
    pass


def check_custom_fields():
    pass


def check_recommended_fields(record, recommended_fields):
    warnings = check_item_is_present(record, recommended_fields)
    if len(warnings) > 0:
        return f"Couldn't find these recommended fields: {', '.join(warnings)}"
    else:
        return None


def collect_ontology_names(json_to_parse):
    ontology_names_to_return = dict()
    for field_name, field_value in json_to_parse['properties'].items():
        if field_name not in SKIP_PROPERTIES and field_value['type'] == 'object':
            if 'text' in field_value['properties'] and 'term' in field_value['properties']:
                ontology_names_to_return[field_name] = field_value['properties']['ontology_name']['const']
        elif field_name not in SKIP_PROPERTIES and field_value['type'] == 'array':
            if 'text' in field_value['items']['properties'] and 'term' in field_value['items']['properties']:
                ontology_names_to_return[field_name] = field_value['items']['properties']['ontology_name']['const']
    return ontology_names_to_return


def do_additional_checks(records, url, name):
    """
    This function will return warning if recommended fields is not present in
    record
    :param records: record to check
    :param url: schema url for this record
    :param name: name of the item to check
    :return: warnings
    """
    issues_to_return = list()
    samples_type_json, samples_core_json = get_samples_json(url)

    # Collect list of recommended fields
    recommended_type_fields = collect_recommended_fields(samples_type_json)
    recommended_core_fields = collect_recommended_fields(samples_core_json)
    # ontology_names_type = collect_ontology_names(samples_type_json)
    ontology_names_core = collect_ontology_names(samples_core_json)

    for index, record in enumerate(records):
        # Get inner issues structure
        record_name = get_record_name(record['custom'], index)
        tmp = get_validation_results_structure(record_name)

        # Check that recommended fields are present
        core_warnings = check_recommended_fields(record['samples_core'],
                                                 recommended_core_fields)
        type_warnings = check_recommended_fields(record,
                                                 recommended_type_fields)
        if core_warnings is not None:
            tmp['core']['warnings'].append(core_warnings)
        if type_warnings is not None:
            tmp['type']['warnings'].append(type_warnings)

        # TODO: Check that ontology text is consistent with ontology term
        check_ontology_text(record['samples_core'], ontology_names_core)

        # TODO: Check that date value is consistent with date units
        check_date_units()

        # TODO: Check breeds
        check_breeds()

        # TODO: Check relationships
        check_relationships()

        # TODO: Check custom fields
        check_custom_fields()

        issues_to_return.append(tmp)
    return issues_to_return


def get_validation_results_structure(record_name):
    """
    This function will create inner validation results structure
    :param record_name: name of the record
    :return: inner validation results structure
    """
    return {
        "name": record_name,
        "core": {
            "errors": list(),
            "warnings": list()
        },
        "type": {
            "errors": list(),
            "warnings": list()
        },
        "custom": {
            "errors": list(),
            "warnings": list()
        }
    }


def get_record_name(record, index):
    """
    This function will return name of the current record or create it
    :param record: record to search name in
    :param index: index for new name creation
    :return: name of the record
    """
    if 'sample_name' not in record:
        return f"record_{index + 1}"
    else:
        return record['sample_name']['value']


def join_issues(to_join_to, first_record, second_record):
    """
    This function will join all issues from first and second record into one
    place
    :param to_join_to: holder that will store merged issues
    :param first_record: first record to get issues from
    :param second_record: second record to get issues from
    :return: merged results
    """
    for issue_type in ['core', 'type', 'custom']:
        for issue in ['errors', 'warnings']:
            to_join_to[issue_type][issue].extend(
                first_record[issue_type][issue])
            to_join_to[issue_type][issue].extend(
                second_record[issue_type][issue])
    return to_join_to
