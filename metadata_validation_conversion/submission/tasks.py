import subprocess
import json
import re
import os
from abc import ABC

from lxml import etree
from elasticsearch import Elasticsearch, RequestsHttpConnection

from metadata_validation_conversion.celery import app
from metadata_validation_conversion.helpers import send_message
from metadata_validation_conversion.constants import ENA_TEST_SERVER, \
    ENA_PROD_SERVER
from metadata_validation_conversion.settings import BOVREG_USERNAME, \
    BOVREG_PASSWORD
from .BiosamplesFileConverter import BiosamplesFileConverter
from .BiosamplesSubmission import BioSamplesSubmission
from .AnalysesFileConverter import AnalysesFileConverter
from .ExperimentsFileConverter import ExperimentFileConverter
from .AnnotateTemplate import AnnotateTemplate
from .helpers import get_credentials
from celery import Task
from django.conf import settings


class LogErrorsTask(Task, ABC):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        send_message(room_id=kwargs['room_id'], submission_message='Error with the submission process',
                     errors=f'Error: {exc}')



@app.task(base=LogErrorsTask)
def get_domains(credentials, room_id):
    send_message(submission_message="Waiting: getting information about "
                                    "existing domains", room_id=room_id)
    username, password = get_credentials(credentials)
    biosamples_submission = BioSamplesSubmission(username, password, {},
                                                 credentials['mode'])
    domains = biosamples_submission.choose_domain()
    if 'Error' in domains:
        send_message(submission_message=domains, room_id=room_id)
    else:
        send_message(
            domains=domains,
            submission_message='Success: got information about existing '
                               'domains', room_id=room_id)
    return 'Success'


@app.task(base=LogErrorsTask)
def submit_new_domain(credentials, room_id):
    username, password = get_credentials(credentials)
    biosamples_submission = BioSamplesSubmission(username, password, {},
                                                 credentials['mode'])
    create_domain_results = biosamples_submission.create_domain(
        credentials['domain_name'], credentials['domain_description'])
    send_message(submission_message=create_domain_results, room_id=room_id)
    domains = biosamples_submission.choose_domain()
    if 'Error' in domains:
        send_message(submission_message=domains, room_id=room_id)
    else:
        send_message(
            domains=domains,
            submission_message='Success: got information about existing '
                               'domains', room_id=room_id)
    return 'Success'


@app.task(base=LogErrorsTask)
def prepare_samples_data(json_to_convert, room_id, private=False, action='submission', mode='prod'):
    conversion_results = BiosamplesFileConverter(json_to_convert[0], private, mode, action)
    results = conversion_results.start_conversion()
    send_message(submission_status='Data is ready', room_id=room_id)
    return results


@app.task(base=LogErrorsTask)
def submit_to_biosamples(results, credentials, room_id, action="submission"):
    send_message(
        submission_message="Waiting: submitting records to BioSamples",
        room_id=room_id)
    username, password = get_credentials(credentials)
    biosamples_submission = BioSamplesSubmission(username, password, results[0],
                                                 credentials['mode'],
                                                 credentials['domain_name'])
    if action == 'update':
        submission_results = biosamples_submission.update_records()
    else:
        submission_results = biosamples_submission.submit_records()

    if 'Error' in submission_results:
        send_message(submission_message=submission_results, room_id=room_id)
        return 'Error'
    else:
        send_message(
            submission_results=submission_results,
            submission_message=f"Success: {action} was completed",
            room_id=room_id)
        submission_data = ''
        for k, v in submission_results.items():
            submission_data += f"{k}\t{v}\n"
        return submission_data


@app.task(base=LogErrorsTask)
def prepare_analyses_data(json_to_convert, room_id, private=False, action='submission'):
    conversion_results = AnalysesFileConverter(json_to_convert[0], room_id,
                                               private, action)
    xml_files = list()
    analysis_xml, submission_xml, sample_xml = \
        conversion_results.start_conversion()
    xml_files.extend([analysis_xml, submission_xml])
    for xml_file in xml_files:
        if 'Error' in xml_file:
            send_message(submission_message='Error: Failed to convert data',
                         errors=xml_file, room_id=room_id)
            return 'Error'
    send_message(submission_status='Data is ready', room_id=room_id)
    return 'Success'


@app.task(base=LogErrorsTask)
def prepare_experiments_data(json_to_convert, room_id, private=False, action='submission'):
    conversion_results = ExperimentFileConverter(json_to_convert[0], room_id,
                                                 private, action)
    xml_files = list()
    experiment_xml, run_xml, study_xml, submission_xml, sample_xml = \
        conversion_results.start_conversion()
    xml_files.extend([experiment_xml, run_xml, study_xml, submission_xml])
    if sample_xml is not None:
        xml_files.append(sample_xml)
    for xml_file in xml_files:
        if 'Error' in xml_file:
            send_message(submission_message='Error: Failed to convert data',
                         errors=xml_file, room_id=room_id)
            return 'Error'
    send_message(submission_status='Data is ready', room_id=room_id)
    return 'Success'


@app.task(base=LogErrorsTask)
def submit_data_to_ena(results, credentials, room_id, submission_type, action="submission"):
    if results[0] != 'Success':
        return 'Error'
    send_message(submission_message='Waiting: submitting records to ENA',
                 room_id=room_id)
    submission_path = ENA_TEST_SERVER if credentials['mode'] == 'test' else \
        ENA_PROD_SERVER
    submission_xml = f"{room_id}_submission.xml"

    # Use BOVREG private account or get credentials from request
    if credentials['private_submission']:
        username = BOVREG_USERNAME
        password = BOVREG_PASSWORD
    else:
        username = credentials["username"]
        password = credentials["password"]
    # public experiments submission
    if submission_type == 'experiments' and \
            not credentials['private_submission']:
        experiment_xml = f"{room_id}_experiment.xml"
        run_xml = f"{room_id}_run.xml"
        study_xml = f"{room_id}_study.xml"
        password = re.escape(password)
        submit_to_ena_process = subprocess.run(
            f'curl -u {username}:{password} '
            f'-F "SUBMISSION=@{submission_xml}" '
            f'-F "EXPERIMENT=@{experiment_xml}" '
            f'-F "RUN=@{run_xml}" '
            f'-F "STUDY=@{study_xml}" '
            f'"{submission_path}"',
            shell=True, capture_output=True)
    # private experiments submission
    elif submission_type == 'experiments' and credentials['private_submission']:
        experiment_xml = f"{room_id}_experiment.xml"
        run_xml = f"{room_id}_run.xml"
        study_xml = f"{room_id}_study.xml"
        sample_xml = f"{room_id}_sample.xml"
        password = re.escape(password)
        submit_to_ena_process = subprocess.run(
            f'curl -u {username}:{password} '
            f'-F "SUBMISSION=@{submission_xml}" '
            f'-F "EXPERIMENT=@{experiment_xml}" '
            f'-F "RUN=@{run_xml}" '
            f'-F "STUDY=@{study_xml}" '
            f'-F "SAMPLE=@{sample_xml}" '
            f'"{submission_path}"',
            shell=True, capture_output=True)
    elif submission_type == 'analyses' \
            and not credentials['private_submission']:
        analysis_xml = f"{room_id}_analysis.xml"
        submit_to_ena_process = subprocess.run(
            f'curl -u {username}:{password} '
            f'-F "SUBMISSION=@{submission_xml}" '
            f'-F "ANALYSIS=@{analysis_xml}" '
            f'"{submission_path}"',
            shell=True, capture_output=True)
    elif submission_type == 'analyses' and credentials['private_submission']:
        # Submit analysis
        analysis_xml = f"{room_id}_analysis.xml"
        sample_xml = f"{room_id}_sample.xml"
        submit_to_ena_process = subprocess.run(
            f'curl -u {username}:{password} '
            f'-F "SUBMISSION=@{submission_xml}" '
            f'-F "ANALYSIS=@{analysis_xml}" '
            f'-F "SAMPLE=@{sample_xml}" '
            f'"{submission_path}"',
            shell=True, capture_output=True)

    submission_results = submit_to_ena_process.stdout
    parsed_results = parse_submission_results(submission_results, submission_type, room_id, action)

    # Adding project to the private data hub
    if parsed_results == 'Success' and credentials['private_submission'] \
            and submission_type == 'experiments':
        project_id = fetch_project_id(submit_to_ena_process.stdout)
        project_json = {
            "projectId": project_id,
            "dcc": "dcc_korman"
        }
        with open(f"{room_id}_project.json", 'w') as w:
            json.dump(project_json, w)
        link_study_process = subprocess.run(
            f'curl -X POST --header "Content-Type: application/json" '
            f'--header "Accept: application/json" '
            f'-u "{BOVREG_USERNAME}:{BOVREG_PASSWORD}" '
            f'-d @{room_id}_project.json '
            f'"https://www.ebi.ac.uk/ena/portal/ams/webin/project/add"',
            shell=True, capture_output=True)
        if 'errorMessage' in link_study_process.stdout.decode('utf-8'):
            error_message = json.loads(
                link_study_process.stdout.decode('utf-8'))['errorMessage']
            send_message(
                submission_message="Error: submission failed",
                submission_results=[[],
                                    [error_message]],
                room_id=room_id)
            return 'Error'
    # TODO: uncomment after testing
    # subprocess.run(f"rm {room_id}*.xml", shell=True)
    return submission_results.decode('utf-8')


def fetch_project_id(submission_results):
    """
    This function returns project id from receipt xml
    :param submission_results: stdout string from subprocess
    :return: project id in format 'PRJEB20767'
    """
    root = etree.fromstring(submission_results)
    return root.find('STUDY').find('EXT_ID').attrib['accession']


def parse_submission_results(submission_results, submission_type, room_id, action="submission"):
    """
    This function parses submission response
    :param submission_results: submission response
    :param submission_type: submission type
    :param room_id: room id to send messages through django channels
    :param action: indicates whether submission to ENA was an update or a new submission
    :return: error and info messages
    """
    if 'Access Denied' in submission_results.decode('utf-8'):
        send_message(submission_message="Error: Access Denied", room_id=room_id)
        return 'Error'
    else:
        root = etree.fromstring(submission_results)
        submission_error_messages = list()
        submission_info_messages = list()
        for messages in root.findall('MESSAGES'):
            for error in messages.findall('ERROR'):
                submission_error_messages.append(error.text)
            for info in messages.findall('INFO'):
                submission_info_messages.append(info.text)
        if len(submission_error_messages) > 0:
            send_message(submission_message=f"Error: {action} failed",
                         submission_results=[submission_info_messages,
                                             submission_error_messages],
                         room_id=room_id)
            return 'Error'
        else:
            # Save submission data to ES
            save_submission_data(root, submission_type, room_id)
            send_message(
                submission_message=f"Success: {action} was successful",
                submission_results=[submission_info_messages],
                room_id=room_id)
            return 'Success'


def parse_experiments_data(root, submission_id):
    object_types = {
        'EXPERIMENT': 'experiments',
        'RUN': 'runs',
        'STUDY': 'studies',
        'PROJECT': 'studies'
    }
    study_objs = []
    if root.get('success') == 'true':
        # get alias and accessions
        submission = root.findall('SUBMISSION')[0]
        submission_data = {
            'alias': submission.get('alias') if submission.get('alias') else '',
            'accession': submission.get('accession') if submission.get('accession') else ''
        }
        for type in object_types:
            objects = root.findall(type)
            if len(objects):
                prop = object_types[type]
                submission_data[prop] = []
                for object in objects:
                    obj_data = {
                        'alias': object.get('alias') if object.get('alias') else '',
                        'accession': object.get('accession') if object.get('accession') else '',
                    }
                    submission_data[prop].append(obj_data)

        # iterate through each study
        for study in submission_data['studies']:
            study_obj = {
                'study_id': study['accession'],
                'study_alias': study['alias'],
                'experiments': [],
                'runs': [],
                'files': [],
                'available_in_portal': 'false'
            }
            # get experiments associated with each study
            experiment_xml = f'{submission_id}_experiment.xml'
            if os.path.exists(experiment_xml):
                experiment_root = etree.parse(experiment_xml).getroot()
                experiments = experiment_root.findall('EXPERIMENT')
                exp_alias_list = []
                assay_types = []
                for exp in experiments:
                    exp_study_alias = exp.findall('STUDY_REF')[0].get('refname')
                    # check that the study reference for the experiment matches the study
                    if exp_study_alias == study['alias']:
                        for experiment in submission_data['experiments']:
                            # get experiment accession from submission data using experiment alias
                            if experiment['alias'] == exp.get('alias'):
                                study_obj['experiments'].append({
                                    'alias': experiment['alias'],
                                    'accession': experiment['accession']
                                })
                                exp_alias_list.append(experiment['alias'])

                        # get assay type
                        attributes = exp.findall('EXPERIMENT_ATTRIBUTES')
                        if len(attributes):
                            attributes = attributes[0].findall('EXPERIMENT_ATTRIBUTE')
                            for attribute in attributes:
                                tag = attribute.findall('TAG')[0].text
                                if tag == 'assay type':
                                    assay_types.append(attribute.findall('VALUE')[0].text)
                study_obj['assay_type'] = ', '.join(set(assay_types))

                # get runs and files associated with the study experiments
                run_xml = f'{submission_id}_run.xml'
                if os.path.exists(run_xml):
                    run_root = etree.parse(run_xml).getroot()
                    runs = run_root.findall('RUN')
                    for r in runs:
                        r_exp_alias = r.findall('EXPERIMENT_REF')[0].get('refname')
                        # get associated runs using experiment_ref
                        if r_exp_alias in exp_alias_list:
                            for run in submission_data['runs']:
                                # get run accession from submission data using run alias
                                if run['alias'] == r.get('alias'):
                                    study_obj['runs'].append({
                                        'alias': run['alias'],
                                        'accession': run['accession']
                                    })
                            # get associated files
                            run_data = r.findall('DATA_BLOCK')
                            if len(run_data):
                                files = run_data[0].findall('FILES')
                                for file in files[0].findall('FILE'):
                                    study_obj['files'].append({
                                        'name': file.get('filename')
                                    })
            study_objs.append(study_obj)
    return study_objs
        
        
def parse_analysis_data(root, submission_id):
    study_objs = []
    if root.get('success') == 'true':
        objects = root.findall('ANALYSIS')
        if len(objects):
            analyses_objs = {}
            for object in objects:
                # get analyses alias and accession
                analysis_alias = object.get('alias')
                analysis_accession = object.get('accession') if object.get('accession') else ''
                analyses_objs[analysis_alias] = {
                    'alias': analysis_alias,
                    'accession': analysis_accession,
                    'study_id': '',
                    'assay_type': ''
                }
            analysis_xml = f'{submission_id}_analysis.xml'
            if os.path.exists(analysis_xml):
                analysis_root = etree.parse(analysis_xml).getroot()
                analyses = analysis_root.findall('ANALYSIS')
                for a in analyses:
                    a_alias = a.get('alias')
                    # get study_id
                    analyses_objs[a_alias]['study_id'] = a.findall('STUDY_REF')[0].get('accession')
                    # get assay_type
                    attributes = a.findall('ANALYSIS_ATTRIBUTES')
                    if len(attributes):
                        attributes = attributes[0].findall('ANALYSIS_ATTRIBUTE')
                        for attribute in attributes:
                            tag = attribute.findall('TAG')[0].text
                            if tag == 'Assay Type':
                                analyses_objs[a_alias]['assay_type'] = attribute.findall('VALUE')[0].text
            study_objs_dict = {}
            for analysis in analyses_objs.values():
                if analysis['study_id'] not in study_objs_dict:
                    study_objs_dict[analysis['study_id']] = {
                        'study_id': analysis['study_id'],
                        'study_alias': '',
                        'assay_type': [],
                        'analyses': [],
                        'available_in_portal': 'false'
                    }
                study_objs_dict[analysis['study_id']]['analyses'].append({
                    'alias': analysis['alias'],
                    'accession': analysis['accession']
                })
                study_objs_dict[analysis['study_id']]['assay_type'].append(analysis['assay_type'])
            for study_obj in study_objs_dict.values():
                study_obj['assay_type'] = ', '.join(set(study_obj['assay_type']))
            study_objs = study_objs_dict.values()
    return study_objs


def save_submission_data(root, submission_type, room_id):
    if root.get('success') == 'true':
        es = Elasticsearch([settings.NODE], connection_class=RequestsHttpConnection, \
                http_auth=(settings.ES_USER, settings.ES_PASSWORD), use_ssl=True, verify_certs=False)
        if submission_type == 'experiments':
            study_objs = parse_experiments_data(root, room_id)
        else:
            study_objs = parse_analysis_data(root, room_id)
        for study_obj in study_objs:
            es.index(index='submissions', id=study_obj['study_id'], body=study_obj)


@app.task(base=LogErrorsTask)
def generate_annotated_template(json_to_convert, room_id, data_type):
    annotation_results = AnnotateTemplate(json_to_convert, room_id, data_type)
    annotation_results.start_conversion()
    send_message(annotation_status='Download data', room_id=room_id)
    return 'Success'
