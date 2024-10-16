from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
import requests
import json
import copy
from elasticsearch import Elasticsearch, RequestsHttpConnection
from ontology_improver.models import User
from django.conf import settings
from datetime import datetime
from django.utils import timezone
import base64
from ontology_improver.utils import *
from ontology_improver.tasks import update_ontology_summary
from metadata_validation_conversion.constants import ZOOMA_SERVICE
from metadata_validation_conversion.helpers import send_message

es = Elasticsearch([settings.NODE], connection_class=RequestsHttpConnection,
                   http_auth=(settings.ES_USER, settings.ES_PASSWORD),
                   use_ssl=True, verify_certs=True)

@csrf_exempt
def get_zooma_ontologies(request):
    if request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    faang_ontologies = '[PATO,BTO,NCIT,LBO,CL,NCBITaxon,EFO,UBERON,OBI,CLO]'
    terms = json.loads(request.body)['terms']
    response = {}
    for term in terms:
        url = f"{ZOOMA_SERVICE}/annotate?propertyValue={term.strip()}&filter=preferred:[FAANG],ontologies:{faang_ontologies}"
        data = requests.get(url).json()
        response[term] = parse_zooma_response(data)
    return JsonResponse(response)


@csrf_exempt
def registration(request):
    if request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
    }
    # parse user info for creating AAP account
    request = json.loads(request.body)
    password = base64.b64decode(request['password'])
    password = password.decode("utf-8")
    post_data = {
        'username': request['username'],
        'password': password,
        'name': request['first_name'] + ' ' + request['last_name'],
        'email': request['email'],
        'organisation': request['organisation']
    }
    res = requests.post("https://api.aai.ebi.ac.uk/auth", \
        data=json.dumps(post_data), headers=headers)
    if res.status_code == 200:
        user_id = res.text
        # save user data
        User.objects.create(user_id = user_id, username = request['username'], \
            first_name = request['first_name'], \
            last_name = request['last_name'], \
            email = request['email'], institute = request['organisation'])
        return JsonResponse({'user_id':user_id})
    else:
        return HttpResponse(res.text, status=res.status_code)

@csrf_exempt
def authentication(request):
    if request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    headers = {
        'Accept': 'text/plain',
        'Authorization': json.loads(request.body)['auth']
    }
    token = requests.get("https://api.aai.ebi.ac.uk/auth", headers=headers).text
    return JsonResponse({'token': token})


def get_user_activity(status_activity, username):
    # sort by timestamp desc and search for username
    sorted_status_activity = sorted(status_activity, key=lambda x: x['timestamp'], reverse=True)
    for ele in sorted_status_activity:
        if ele['user'] == username:
            return ele
    return None


def remove_user_activity(status_activity, username, status):
    filtered_status_activity = list(
        filter(
            lambda obj: not (obj['user'] == username and obj['status'] == status),
            status_activity
        )
    )
    return filtered_status_activity


def format_timestamp(timestamp_str):
    return timestamp_str.replace('T', ' ').replace('+', ' +').split('.')[0]


@csrf_exempt
def validate_ontology(request, room_id):
    if request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    data = json.loads(request.body)
    ontology = data['ontology']

    # check if user's last action is the same as the current one, reject if so
    status_activity = ontology['status_activity']
    user_last_action = get_user_activity(status_activity, data['user'])
    if user_last_action is not None:
        if user_last_action['status'] == data['status']:
            send_message(room_id=room_id, ontology_update_status=f"You last marked this term as "
                                                                 f"{user_last_action['status']} on "
                                                                 f"{format_timestamp(user_last_action['timestamp'])}")
            return HttpResponse(status=409)

    # proceed if user's last action is different from the current one or user hasn't validated that term yet
    res = es.search(index="ontologies", body={"query": {"match": {"_id": ontology['key']}}})
    if len(res['hits']['hits']) == 0:
        return HttpResponse(status=404)

    if data['status'].lower() == 'verified':
        update_payload = {
            'upvotes_count': ontology['upvotes_count'] + 1
        }
        # if user has previously down-voted that term
        if user_last_action is not None and user_last_action['status'].lower() == 'needs improvement':
            update_payload.update({'downvotes_count': ontology['downvotes_count'] - 1})
            status_activity = remove_user_activity(status_activity, data['user'], user_last_action['status'])
    else:
        update_payload = {
            'downvotes_count': ontology['downvotes_count'] + 1
        }
        # if user has previously up-voted that term
        if user_last_action is not None and user_last_action['status'].lower() == 'verified':
            update_payload.update({'upvotes_count': ontology['upvotes_count'] - 1})
            status_activity = remove_user_activity(status_activity, data['user'], user_last_action['status'])

    # add latest user action to status activity
    status_activity.append({
        'project': data['project'],
        'status': data['status'],
        'timestamp': datetime.now(tz=timezone.utc),
        'user': data['user'],
        'comments': data['user_comments']
    })
    update_payload.update({'status_activity': status_activity})
    es.update(index='ontologies', id=ontology['key'], body={"doc": update_payload})

    if data['status'].lower() == 'needs improvement':
        # create new record with suggested changes
        new_ontology = copy.deepcopy(ontology)
        new_ontology['key'] = f"{ontology['term']}-{ontology['id']}"
        new_ontology['projects'] = data['project'] if data['project'] else []
        new_ontology['upvotes_count'] = 0
        new_ontology['downvotes_count'] = 0
        new_ontology['status_activity'] = [{
            'project': data['project'],
            'status': 'Awaiting Assessment',
            'timestamp': datetime.now(tz=timezone.utc),
            'user': data['user']
        }]
        # create new record only if 'key' doesn't exist as document id in ES index
        res = es.search(index="ontologies", body={"query": {"match": {"_id": new_ontology['key']}}})
        if len(res['hits']['hits']) == 0:
            es.index(index='ontologies', id=new_ontology['key'], body=new_ontology)

    # task = update_ontology_summary.s().set(queue='submission')
    # task_chain = chain(task)
    # task_chain.apply_async()
    return HttpResponse(status=200)


@csrf_exempt
def ontology_updates(request):
    if request.method != 'POST':
        return HttpResponse("This method is not allowed!\n")
    data = json.loads(request.body)
    ontologies = data['ontologies']
    # get user info
    user = data['user']

    for ontology in ontologies:
        # url = f"{BE_SVC}/data/ontologies/{ontology['key']}"
        # res = requests.get(url)
        res = es.search(index="ontologies", body={"query": {"match": {"_id": ontology['key']}}})
        # create new ontology if ontology does not exist
        # if res.status_code != 200 or len(json.loads(res.content)['hits']['hits']) == 0:
        if len(res['hits']['hits']) == 0:
            new_ontology = copy.deepcopy(ontology)
            new_ontology['key'] = f"{ontology['term']}-{ontology['id']}"
            new_ontology['upvotes_count'] = 0
            new_ontology['downvotes_count'] = 0
            new_ontology['status_activity'] = [{
                'project': ontology['projects'],
                'status': 'Awaiting Assessment',
                'timestamp': datetime.now(tz=timezone.utc),
                'user': user
            }]
            es.index(index='ontologies', id=new_ontology['key'], body=new_ontology)
        # edit ontology if it already exists
        else:
            # existing_ontology = json.loads(res.content)['hits']['hits'][0]['_source']
            existing_ontology = res['hits']['hits'][0]['_source']
            update_payload = {
                'type': ontology['type'],
                'synonyms': ontology['synonyms'],
                'summary': ontology['summary'] if "summary" in ontology else "",
                'projects': ontology['projects'],
                'species': ontology['species'],
                'tags': ontology['tags']
            }
            es.update(index='ontologies', id=existing_ontology['key'], body={"doc": update_payload})
    # update summary statistics
    # TODO: check this task
    # task = update_ontology_summary.s().set(queue='submission')
    # task_chain = chain(task)
    # res = task_chain.apply_async()
    return HttpResponse(status=201)
