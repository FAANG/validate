import json
from django.conf import settings
from elasticsearch import Elasticsearch, RequestsHttpConnection
from metadata_validation_conversion.celery import app
import itertools

es = Elasticsearch([settings.NODE], connection_class=RequestsHttpConnection,
                   http_auth=(settings.ES_USER, settings.ES_PASSWORD),
                   use_ssl=True, verify_certs=True)


# generate field 'type_counts' list of entries based on ontology['type']
# params: ontology_type: "type": [ "organismPart", "cellType"]
# returns: "type_counts": [{"type": "organismPart", "count": 34}, ...]
def generate_type_counts(ontology_type, type_counts):
    for type in ontology_type:
        # check if type is found in type_counts list
        if any(obj['type'] == type for obj in type_counts):
            index = -1
            for i, obj in enumerate(type_counts):
                if obj['type'] == type:
                    index = i
                    break
            type_counts[index]['count'] = type_counts[index]['count'] + 1
        else:
            type_counts.append({'type': type,
                                'count': 1})


# update created_edited_count
def generate_created_edited_count(activity):
    activity['created_edited_count'] = activity['created_edited_count'] + 1


# fetch ES records based on query and index provided
def es_fetch_records(index, filters):
    count = 0
    recordset = []

    while True:
        res = es.search(index=index, size=50000, from_=count,
                        track_total_hits=True, body=json.loads(filters))
        count += 50000
        records = list(map(lambda rec: rec['_source'], res['hits']['hits']))
        recordset += records

        if count > res['hits']['total']['value']:
            break
    return recordset


# fetch organisms (species) associated with project
def fetch_project_species(project):
    query = {'query': {'bool': {'filter': [{'terms': {'secondaryProject': [project]}}]}}}
    records = es_fetch_records("organism", json.dumps(query))
    species = filter(lambda x: x is not None,
                     set(map(lambda rec: rec['organism']['text'], records)))
    species_str = ', '.join(str(s) for s in species)
    return species_str


# fetch latest activity for each user form list provided
def get_latest_status_activity(status_activity):
    latest_activity_list = []
    sorted_status_activity = sorted(status_activity, key=lambda x: x['timestamp'], reverse=True)
    activity_users = set(map(lambda d: d['user'], sorted_status_activity))
    for user in activity_users:
        gen = (
            activity for activity in sorted_status_activity
            if activity["user"] == user
        )
        latest_activity_list.append(next(gen))

    return latest_activity_list


# generate users latest activity for each project
def generate_activity_counts(project):
    query = {'query': {'bool': {'filter': [{'terms': {'projects': [project]}}]}}}
    records = es_fetch_records("ontologies", json.dumps(query))
    status_activity_list = list(
        map(lambda rec: get_latest_status_activity(rec['status_activity']), records))

    # merge list of lists into one list
    status_activity_list = list(itertools.chain.from_iterable(status_activity_list))

    verified_records = list(filter(lambda d: 'status' in d and d['status'].lower() == 'verified',
                                   status_activity_list))
    needs_improvement_records = list(filter(lambda d: 'status' in d and d['status'].lower() == 'needs improvement',
                                            status_activity_list))

    return verified_records, needs_improvement_records


@app.task
# Update the summary_ontologies index based on data from the ontologies index
def update_ontology_summary():
    query = {"query":
        {
            "regexp": {
                "projects": ".+"
            }
        }
    }
    records = es_fetch_records("ontologies", json.dumps(query))
    project_dict = {}

    for rec in records:
        projects_list = rec['projects']
        for proj in projects_list:
            if proj in project_dict:
                # update dict
                generate_type_counts(rec['type'], project_dict[proj]['type_counts'])
                generate_created_edited_count(project_dict[proj]['activity'])
            else:
                project_dict[proj] = {
                    "project": proj,
                    "species": "",
                    "type_counts": [],
                    "activity": {'created_edited_count': 0, 'validated_count': 0, 'downvoted_count': 0}
                }
                generate_type_counts(rec['type'], project_dict[proj]['type_counts'])
                generate_created_edited_count(project_dict[proj]['activity'])
                project_dict[proj]['species'] = fetch_project_species(proj)
                activity_counts_verified, activity_counts_needs_improvement = generate_activity_counts(proj)
                project_dict[proj]['activity']['validated_count'] = len(activity_counts_verified)
                project_dict[proj]['activity']['downvoted_count'] = len(activity_counts_needs_improvement)

    # update index
    for project in project_dict:
        print("updated_project_stats: ", project_dict[project])
        es.index(index='summary_ontologies', id=project, body=project_dict[project])

    return "Success"
