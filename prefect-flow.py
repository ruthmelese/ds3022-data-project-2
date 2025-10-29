from prefect import flow, get_run_logger
import time
import os
import boto3
import requests

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SUBMIT_Q = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
EXPECTED = 21
POLL = 8
TIMEOUT = 20 * 60

def sqs():
    return boto3.client("sqs", region_name=REGION)

@flow(name="dp2-quote-assembler")
def dp2(uvaid: str):
    log = get_run_logger()

    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    qurl = r.json()["sqs_url"]
    log.info(f"Queue created for {uvaid}: {qurl}")

    client = sqs()
    start = time.time()
    while True:
        attrs = client.get_queue_attributes(
            QueueUrl=qurl,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]
        vis = int(attrs.get("ApproximateNumberOfMessages", "0"))
        if vis > 0:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError("Timed out waiting for messages.")
        time.sleep(POLL)

    pairs = []
    last = time.time()
    while True:
        resp = client.receive_message(
            QueueUrl=qurl,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            if len(pairs) >= EXPECTED:
                break
            if time.time() - last > TIMEOUT:
                raise TimeoutError("Still missing messages after timeout.")
            time.sleep(2)
            continue
        for m in msgs:
            mattrs = m.get("MessageAttributes") or {}
            if "order_no" in mattrs and "word" in mattrs:
                order = int(mattrs["order_no"]["StringValue"])
                word  = mattrs["word"]["StringValue"]
                pairs.append((order, word))
                last = time.time()
            client.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])
        if len(pairs) >= EXPECTED:
            break

    if not pairs:
        raise ValueError("No message fragments received.")

    phrase = " ".join(w for _, w in sorted(pairs, key=lambda t: t[0]))
    log.info(f"Phrase: {phrase}")

    resp = client.send_message(
        QueueUrl=SUBMIT_Q,
        MessageBody="solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": uvaid},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "prefect"},
        },
    )
    code = resp["ResponseMetadata"]["HTTPStatusCode"]
    log.info(f"Submitted. HTTP {code}")
    print("PHRASE:", phrase)
    print("SUBMIT_STATUS:", code)

if __name__ == "__main__":
    dp2("cup6cd")

