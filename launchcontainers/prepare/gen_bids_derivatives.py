'''
This code is used to generate BIDS-valid derivatives folder to make it able to index by pybids

right now it will put a dataset_description.json under the derivatives/pipeline/


'''
import os
import json
import pandas as pd
import logging
logger = logging.getLogger(__name__)

def gen_dataset_desc_json(output_dir):
    os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist

    dataset_description = {
        'Acknowledgements': 'TODO: whom you want to acknowledge',
        'Authors': ['TODO:', 'First1 Last1', 'First2 Last2', '...'],
        'BIDSVersion': '1.10.0',
        'DatasetDOI': 'TODO: eventually a DOI for the dataset',
        'Funding': ['TODO', 'GRANT #1', 'GRANT #2'],
        'HowToAcknowledge': 'TODO: describe how to acknowledge -- either cite a corresponding paper, \
        or just in acknowledgement section',
        'License': 'TODO: choose a license, e.g. PDDL (http://opendatacommons.org/licenses/pddl/)',
        'Name': 'TODO: name of the dataset',
        'ReferencesAndLinks': ['TODO', 'List of papers or websites']
    }

    output_path = os.path.join(output_dir, 'dataset_description.json')
    with open(output_path, 'w') as f:
        json.dump(dataset_description, f, indent=4)

    logger.info(f"dataset_description.json created at: {output_path}")

def gen_participant_json(output_dir):
    os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist

    participant = {
                "participant_id": {
                    "Description": "Participant identifier"
                },
                "age": {
                    "Description": "Age in years (TODO - verify) as in the initial session, \
                    might not be correct for other sessions"
                },
                "sex": {
                    "Description": "self-rated by participant, M for male/F for female (TODO: verify)"
                },
                "group": {
                    "Description": "(TODO: adjust - by default everyone is in control group)"
                }
                }


    output_path = os.path.join(output_dir, 'participant.json')
    with open(output_path, 'w') as f:
        json.dump(participant, f, indent=4)

    logger.info(f"participant.json created at: {output_path}")

def gen_readme(output_dir, content=""):

    os.makedirs(output_dir, exist_ok=True)
    readme_path = os.path.join(output_dir, "README")

    with open(readme_path, "w") as f:
        f.write(content)

    logger.info(f"Empty README created at: {readme_path}")


def gen_participant_tsv(output_dir):
    os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist

    if columns is None:
        columns = ["participant_id", "age", "sex", "group"]

    participants_path = os.path.join(output_dir, "participants.tsv")

    df = pd.DataFrame(columns=columns)
    df.to_csv(participants_path, sep="\t", index=False)

    logger.info(f"participants.tsv created at: {participants_path}")