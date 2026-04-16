from __future__ import annotations

import json
import os
import threading
import time
import uuid

import grpc

import gateway_pb2  # generated at image build time
import gateway_pb2_grpc  # generated at image build time


def _env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        return default
    return value


def _env_choice(name: str, default: str, allowed: set[str]) -> str:
    value = _env(name, default).upper()
    if value not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}, got {value!r}")
    return value


def _json(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class Worker:
    def __init__(self, stub: gateway_pb2_grpc.GatewayStub, worker_name: str, business_key: str) -> None:
        self.stub = stub
        self.worker_name = worker_name
        self.business_key = business_key
        self.stop = threading.Event()

        self.process_id = _env("BPMN_PROCESS_ID", "payment-process")
        self.fraud_message_name = _env("FRAUD_MESSAGE_NAME", "fraudResultMessage")
        self.fraud_result = _env_choice("FRAUD_RESULT", "APPROVE", {"APPROVE", "REJECT", "MANUAL"})
        self.manual_result = _env_choice("MANUAL_RESULT", "APPROVE", {"APPROVE", "REJECT"})
        self.transfer_mode = _env_choice("TRANSFER_MODE", "OK", {"OK", "FAIL"})

        self.job_timeout_ms = int(_env("JOB_TIMEOUT_MS", "60000"))
        self.activate_max = int(_env("ACTIVATE_MAX", "10"))
        self.poll_sleep_s = float(_env("POLL_SLEEP_S", "0.2"))

        self.job_types = [
            "createPayment",
            "holdMoney",
            "startFraud",
            "reserveMoney",
            "transfer",
            "confirm",
            "notify",
            "audit",
            "rejectPayment",
            "releaseMoney",
            # Эмуляция ручной таски
            "io.camunda.zeebe:userTask",
        ]

    def run(self) -> None:
        while not self.stop.is_set():
            any_job = False
            for job_type in self.job_types:
                if self.stop.is_set():
                    break
                got = self._activate_and_handle(job_type)
                any_job = any_job or got
            if not any_job:
                time.sleep(self.poll_sleep_s)

    def _activate_and_handle(self, job_type: str) -> bool:
        request = gateway_pb2.ActivateJobsRequest(
            type=job_type,
            worker=self.worker_name,
            timeout=self.job_timeout_ms,
            maxJobsToActivate=self.activate_max,
            requestTimeout=-1,
        )

        got_any = False
        for response in self.stub.ActivateJobs(request):
            for job in response.jobs:
                got_any = True
                self._handle_job(job)
        return got_any

    def _complete(self, job_key: int, variables: dict | None = None) -> None:
        req = gateway_pb2.CompleteJobRequest(jobKey=job_key, variables=_json(variables or {}))
        self.stub.CompleteJob(req)

    def _publish_fraud_approve(self) -> None:
        req = gateway_pb2.PublishMessageRequest(
            name=self.fraud_message_name,
            correlationKey=self.business_key,
            timeToLive=int(_env("MESSAGE_TTL_MS", "300000")),
            messageId=str(uuid.uuid4()),
            variables=_json({"fraudResult": self.fraud_result}),
        )
        self.stub.PublishMessage(req)

    def _handle_job(self, job: gateway_pb2.ActivatedJob) -> None:
        job_key = job.key
        job_type = job.type
        # Эмуляция ручной таски
        if job_type == "io.camunda.zeebe:userTask":
            if job.elementId != "manualTask":
                print(f"[job] userTask elementId={job.elementId} key={job_key} (skipping)")
                return
            print(f"[job] manualTask(userTask) key={job_key} manualResult={self.manual_result}")
            self._complete(job_key, {"manualResult": self.manual_result})
            return

        if job_type == "createPayment":
            variables = {"paymentId": str(uuid.uuid4()), "amount": 1000, "status": "CREATED", "businessKey": self.business_key}
            print(f"[job] createPayment key={job_key}")
            self._complete(job_key, variables)
            return

        if job_type == "holdMoney":
            print(f"[job] holdMoney key={job_key}")
            self._complete(job_key, {"holdSuccess": True})
            return

        if job_type == "startFraud":
            print(
                f"[job] startFraud key={job_key} -> publish message '{self.fraud_message_name}' fraudResult={self.fraud_result}"
            )
            self._publish_fraud_approve()
            self._complete(job_key, {})
            return

        if job_type == "reserveMoney":
            print(f"[job] reserveMoney key={job_key}")
            self._complete(job_key, {"reserveSuccess": True})
            return

        if job_type == "transfer":
            if self.transfer_mode == "FAIL":
                print(f"[job] transfer key={job_key} -> fail (transferSuccess=false)")
                self._complete(job_key, {"transferSuccess": False})
                return
            print(f"[job] transfer key={job_key}")
            self._complete(job_key, {"transferSuccess": True})
            return

        if job_type == "confirm":
            print(f"[job] confirm key={job_key}")
            self._complete(job_key, {"status": "CONFIRMED"})
            return

        if job_type == "notify":
            print(f"[job] notify key={job_key}")
            self._complete(job_key, {})
            return

        if job_type == "audit":
            print(f"[job] audit key={job_key}")
            self._complete(job_key, {})
            return

        if job_type == "rejectPayment":
            print(f"[job] rejectPayment key={job_key}")
            self._complete(job_key, {"status": "REJECTED"})
            return

        if job_type == "releaseMoney":
            print(f"[job] releaseMoney key={job_key}")
            self._complete(job_key, {"status": "RELEASED"})
            return

        print(f"[job] unknown type={job_type} key={job_key} (completing empty)")
        self._complete(job_key, {})


def main() -> int:
    zeebe_address = _env("ZEEBE_ADDRESS", "zeebe:26500")
    worker_name = _env("WORKER_NAME", "py-hapdemo")

    business_key = _env("BUSINESS_KEY", str(uuid.uuid4()))
    print(f"[cfg] ZEEBE_ADDRESS={zeebe_address} BUSINESS_KEY={business_key}")

    channel = grpc.insecure_channel(zeebe_address)
    stub = gateway_pb2_grpc.GatewayStub(channel)

    worker = Worker(stub=stub, worker_name=worker_name, business_key=business_key)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()

    request = gateway_pb2.CreateProcessInstanceRequest(
        bpmnProcessId=worker.process_id,
        version=-1,
        variables=_json({"businessKey": business_key}),
    )
    with_result = gateway_pb2.CreateProcessInstanceWithResultRequest(
        request=request,
        requestTimeout=int(_env("PROCESS_TIMEOUT_MS", "60000")),
        fetchVariables=["status", "paymentId", "amount", "fraudResult", "holdSuccess", "reserveSuccess", "transferSuccess"],
    )

    print(f"[start] processId={worker.process_id}")
    result = stub.CreateProcessInstanceWithResult(with_result)
    worker.stop.set()
    thread.join(timeout=2)

    print(f"[done] processInstanceKey={result.processInstanceKey}")
    print(f"[vars] {result.variables}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
