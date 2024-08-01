"""
This code runs on AWS Lambda to format a notification
and send it to SNS upon completion of a DQ Ruleset.
"""
import os
import json
import boto3


def get_execution_time(start_time, end_time):
    """
    Calculates the execution runtime of the ruleset.
    :param start_time: start time of the ruleset - <class 'datetime.datetime'>
    :param end_time: end time of the ruleset - <class 'datetime.datetime'>
    :return: execution runtime of the ruleset - <> days <1-23> hours <1-59> minutes <1-59> seconds
    """
    time_difference = end_time - start_time
    days = time_difference.days
    hours, remainder = divmod(time_difference.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    execution_time = ' '.join(filter(None, [
        f"{days} days" if days else "",
        f"{hours} hours" if hours else "",
        f"{minutes} minutes" if minutes else "",
        f"{seconds} seconds" if seconds else ""
    ]))

    return execution_time


# pylint: disable=line-too-long, unused-argument, too-many-locals, too-many-statements
def lambda_handler(event, context):
    """
    Formats the ruleset response and publishes to SNS topic
    :param event: information on event that triggers lambda
    :param context: not used
    :return: 200 if success, otherwise exception
    """
    environment = os.environ.get('ENVIRONMENT')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    sns_client = boto3.client('sns')
    glue_client = boto3.client('glue')

    message_text = ""
    subject_text = ""
    try:
        if event['detail']['context']['contextType'] == 'GLUE_DATA_CATALOG':
            message_text += "Glue Data Quality run details:\n"
            message_text += f"Ruleset Name: {str(event['detail']['rulesetNames'][0])}\n"
            message_text += f"Glue Table Name: {str(event['detail']['context']['tableName'])}\n"
            message_text += f"Glue Database Name: {str(event['detail']['context']['databaseName'])}\n"
            message_text += f"Run ID: {str(event['detail']['context']['runId'])}\n"
            message_text += f"Result ID: {str(event['detail']['resultId'])}\n"
            message_text += f"State: {str(event['detail']['state'])}\n"
            message_text += f"Score: {str(event['detail']['score'])}\n"
            message_text += f"No of rules succeeded: {str(event['detail']['numRulesSucceeded'])}\n"
            message_text += f"No of rules failed: {str(event['detail']['numRulesFailed'])}\n"
            message_text += f"No of rules skipped: {str(event['detail']['numRulesSkipped'])}\n"

            subject_text = f"[{environment.upper()}] Glue DQ Ruleset - {str(event['detail']['rulesetNames'][0])} run details"

        result_id = str(event['detail']['resultId'])
        print("Result ID: ", result_id)
        response = glue_client.get_data_quality_result(ResultId=result_id)

        rule_results = response['RuleResults']
        ruleset_start_time = response['StartedOn']
        ruleset_end_time = response['CompletedOn']
        rule_execution_time = get_execution_time(ruleset_start_time, ruleset_end_time)
        message_text += f"Ruleset execution time: {rule_execution_time}\n"

        message_text += "\n\nRuleset details evaluation steps results:\n\n"
        subresult_info = []

        for ruleset in rule_results:
            subresult = f"Name: {ruleset['Name']}\t\tResult: {ruleset['Result']}\t\tDescription: \t{ruleset['Description']}"
            if 'EvaluationMessage' in ruleset:
                subresult += f"\t\tEvaluationMessage: {ruleset['EvaluationMessage']}"
            subresult_info.append({
                'Name': ruleset['Name'],
                'Result': ruleset['Result'],
                'Description': ruleset['Description'],
                'EvaluationMessage': ruleset.get('EvaluationMessage', '')
            })
            message_text += "\n" + subresult

        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message_text,
            Subject=subject_text
        )
    except Exception as exception:
        raise ValueError('ERROR - ran into error while parsing ruleset or publishing to SNS', exception) from exception

    return {
        'statusCode': 200,
        'body': json.dumps('Message published to SNS topic')
    }
