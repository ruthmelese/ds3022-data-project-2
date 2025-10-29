from prefect import flow, get_run_logger
import time, os, boto3, requests

# AWS and  project configuration
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SUBMIT_Q = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
EXPECTED = 21          # number of messages per assignment instructions
TIMEOUT = 20 * 60      # overall safety timeout (20 min)

# helper: function
def sqs():
    return boto3.client("sqs", region_name=REGION)


@flow(name="dp2-quote-assembler")
def dp2(uvaid: str):
    log = get_run_logger()

    # Makes a POST  request to the class API
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    qurl = r.json()["sqs_url"]                 # queue unique to this UVA ID
    log.info(f"Queue created for {uvaid}: {qurl}")

    client = sqs()
    pairs = []                 # store tuples
    last_attr_check = 0        # track last attribute query time
    attr_every = 30            # seconds between queue attribute checks
    long_poll = 20             # long-poll duration
    start = time.time()

    # Loop continues until all 21 messages are retrieved
    while len(pairs) < EXPECTED:
        # Long-poll to stream messages as they become visible
        resp = client.receive_message(
            QueueUrl=qurl,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=long_poll,
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])

        # Parse attributes from each message
        for m in msgs:
            a = m.get("MessageAttributes") or {}
            if "order_no" in a and "word" in a:
                order = int(a["order_no"]["StringValue"])
                word  = a["word"]["StringValue"]
                pairs.append((order, word))
                log.info(f"Collected {len(pairs)}/{EXPECTED} · latest='{word}' (#{order})")
            client.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])

        # Every 30s, check remaining delayed messages for precision monitoring
        now = time.time()
        if now - last_attr_check >= attr_every:
            attrs = client.get_queue_attributes(
                QueueUrl=qurl,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )["Attributes"]
            vis  = int(attrs.get("ApproximateNumberOfMessages", "0"))
            infl = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
            dly  = int(attrs.get("ApproximateNumberOfMessagesDelayed", "0"))
            log.info(f"Status · visible={vis} inflight={infl} delayed={dly} · collected={len(pairs)}/{EXPECTED}")
            last_attr_check = now

        if now - start > TIMEOUT:
            raise TimeoutError(f"Timeout: only collected {len(pairs)}/{EXPECTED} within {TIMEOUT}s")

    if not pairs:
        raise ValueError("No message fragments received.")

    phrase = " ".join(w for _, w in sorted(pairs, key=lambda t: t[0]))
    log.info(f"Phrase reconstructed: {phrase}")

    # Submit phrase, UVA ID, and platform through  SQS message
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


# entry point for local testing
if __name__ == "__main__":
    dp2("cup6cd")

