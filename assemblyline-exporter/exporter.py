import os

from prometheus_client import start_http_server, Gauge
from assemblyline_client import get_client as assemblyline_get_client
from dotenv import load_dotenv


HEARTBEAT = Gauge("assemblyline_last_heartbeat", "Last heartbeat", ["component"])
COMPONENT_INSTANCES = Gauge(
    "assemblyline_component_instances", "Number of instances", ["component"]
)

INGESTER_QUEUES = Gauge(
    "assemblyline_ingester_queues", "Size of ingester queues", ["name"]
)

BYTES_COMPLETED = Gauge("assemblyline_bytes_ingested", "Bytes completed")
BYTES_INGESTED = Gauge("assemblyline_bytes_completed", "Bytes ingested")
FILES_COMPLETED = Gauge("assemblyline_files_completed", "Files finished analyzing")
SUBMISSIONS_COMPLETED = Gauge(
    "assemblyline_submissions_completed", "Submissions completed"
)
SUBMISSIONS_INGESTED = Gauge(
    "assemblyline_submissions_ingested", "Submissions ingested"
)

SERVICE_INSTANCES_RUNNING = Gauge(
    "assemblyline_service_instances_running", "Number of running instances", ["service"]
)
SERVICE_INSTANCES_TARGET = Gauge(
    "assemblyline_service_instances_target",
    "Target number of running instances",
    ["service"],
)
SERVICE_INSTANCES_MIN = Gauge(
    "assemblyline_service_instances_minimum",
    "Minimum number of running services",
    ["service"],
)
SERVICE_INSTANCES_MAX = Gauge(
    "assemblyline_service_instances_maximum",
    "Maximum number of running instances",
    ["service"],
)
SERVICE_INSTANCES_DYN_MAX = Gauge(
    "assemblyline_service_instances_dynamic_maximum",
    "Service dynamic_maximum",
    ["service"],
)
SERVICE_QUEUE = Gauge("assemblyline_service_queue", "Service queue", ["service"])
SERVICE_PRESSURE = Gauge(
    "assemblyline_service_pressure", "Service pressure", ["service"]
)
SERVICE_DUTY_CYCLE = Gauge(
    "assemblyline_service_duty_cycle", "Service duty cycle", ["service"]
)

SERVICE_BUSY = Gauge(
    "assemblyline_service_busy", "Number of instances busy", ["service"]
)
SERVICE_IDLE = Gauge(
    "assemblyline_service_idle", "Number of instances idle", ["service"]
)
SERVICE_FAILURES = Gauge(
    "assemblyline_service_failures", "Number of service failures", ["service", "type"]
)
SERVICE_PROCESSED = Gauge(
    "assemblyline_service_processed",
    "Number of samples processed by the service",
    ["service"],
)

# Callback function when an alerter message is received
def alerter_msg_callback(msg):
    HEARTBEAT.labels("alerter").set_to_current_time()
    # {"instances": 1, "metrics": {"created": 0, "error": 0, "received": 0, "updated": 0}, "queues": {"alert": 0}}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("alerter").set(instance_count)


# Callback function when an archive message is received
def archive_msg_callback(msg):
    HEARTBEAT.labels("archive").set_to_current_time()
    # {"instances": 1, "metrics": {"alert": 0, "cached_file": 0, "emptyresult": 0, "error": 0, "file": 0, "filescore": 0, "result": 0, "submission": 0, "submission_tree": 0, "submission_summary": 0}}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("archive").set(instance_count)


# Callback function when a dispatcher message is received
def dispatcher_msg_callback(msg):
    HEARTBEAT.labels("dispatcher").set_to_current_time()
    # {"inflight": {"max": 10000, "outstanding": 6000, "per_instance": [860, 802, 845, 933, 560, 948, 1052]}, "instances": 7, "metrics": {"files_completed": 3247, "submissions_completed": 310, "service_timeouts": 24, "cpu_seconds": 0.0012159556964380675, "cpu_seconds_count": 76304, "busy_seconds": 0.0017459407076942618, "busy_seconds_count": 76304}, "queues": {"ingest": 0, "start": [0, 0, 0, 0, 0, 0, 0], "result": [0, 0, 0, 0, 0, 0, 0], "command": [0, 0, 0, 0, 0, 0, 0]}, "component": "dispatcher"}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("dispatcher").set(instance_count)


# Callback function when an expiry message is received
def expiry_msg_callback(msg):
    HEARTBEAT.labels("expiry").set_to_current_time()
    # {"instances": 1, "metrics": {"alert": 0, "cached_file": 2, "emptyresult": 0, "error": 0, "file": 0, "filescore": 0, "result": 0, "submission": 0, "submission_tree": 0, "submission_summary": 0}, "queues": {"alert": 0, "cached_file": 0, "emptyresult": 0, "error": 0, "file": 0, "filescore": 0, "result": 0, "submission": 0, "submission_tree": 0, "submission_summary": 0}}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("expiry").set(instance_count)


# Callback function when an ingest message is received
def ingest_msg_callback(msg):
    HEARTBEAT.labels("ingester").set_to_current_time()
    # {"instances": 1, "metrics": {"cache_miss": 298, "cache_expired": 0, "cache_stale": 1, "cache_hit_local": 3, "cache_hit": 0, "bytes_completed": 2033061545, "bytes_ingested": 716394989, "duplicates": 8, "error": 0, "files_completed": 6040, "skipped": 0, "submissions_completed": 293, "submissions_ingested": 53, "timed_out": 0, "whitelisted": 0, "cpu_seconds": 0.003922335950595493, "cpu_seconds_count": 2753, "busy_seconds": 0.024584957525334357, "busy_seconds_count": 2753}, "processing": {"inflight": 6000}, "processing_chance": {"critical": 1, "high": 1, "low": 1, "medium": 1}, "queues": {"critical": 0, "high": 0, "ingest": 0, "complete": 0, "low": 0, "medium": 61187}}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("ingester").set(instance_count)

    metrics = msg.get("metrics")

    BYTES_COMPLETED.set(metrics.get("bytes_completed"))
    BYTES_INGESTED.set(metrics.get("bytes_ingested"))
    FILES_COMPLETED.set(metrics.get("files_completed"))
    SUBMISSIONS_COMPLETED.set(metrics.get("submissions_completed"))
    SUBMISSIONS_INGESTED.set(metrics.get("submissions_ingested"))

    queues = msg.get("queues")
    for k in queues:
        INGESTER_QUEUES.labels(k).set(queues[k])


# Callback function when an scaler message is received
def scaler_msg_callback(msg):
    HEARTBEAT.labels("scaler").set_to_current_time()
    # {"instances": 1, "metrics": {"memory_free": 781668.1835647193, "cpu_free": -410.5142530017242, "memory_total": 1639450.140625, "cpu_total": 416.0}}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("scaler").set(instance_count)


# Callback function when a scaler status message is received
def scaler_status_msg_callback(msg):
    HEARTBEAT.labels("scaler").set_to_current_time()
    # {"service_name": "APKaye", "metrics": {"running": 16, "target": 138, "minimum": 4, "maximum": 0, "dynamic_maximum": 148, "queue": 833, "pressure": 17.01167246391921, "duty_cycle": 0.09375}}
    service_name = msg.get("service_name").lower()
    metrics = msg.get("metrics")

    running = metrics.get("running")
    SERVICE_INSTANCES_RUNNING.labels(service_name).set(running)

    target = metrics.get("target")
    SERVICE_INSTANCES_TARGET.labels(service_name).set(target)

    minimum = metrics.get("minimum")
    SERVICE_INSTANCES_MIN.labels(service_name).set(minimum)

    maximum = metrics.get("maximum")
    SERVICE_INSTANCES_MAX.labels(service_name).set(maximum)

    dynamic_maximum = metrics.get("dynamic_maximum")
    SERVICE_INSTANCES_DYN_MAX.labels(service_name).set(dynamic_maximum)

    queue = metrics.get("queue")
    SERVICE_QUEUE.labels(service_name).set(queue)

    pressure = metrics.get("pressure")
    SERVICE_PRESSURE.labels(service_name).set(pressure)

    duty_cycle = metrics.get("duty_cycle")
    SERVICE_DUTY_CYCLE.labels(service_name).set(duty_cycle)


# Callback function when a service message is received
def service_msg_callback(msg):
    # {"activity": {"busy": 13, "idle": 0}, "instances": 13, "metrics": {"cache_hit": 0, "cache_miss": 1, "cache_skipped": 0, "execute": 1, "fail_recoverable": 4, "fail_nonrecoverable": 0, "scored": 1, "not_scored": 0}, "queue": 835, "service_name": "APKaye"}
    instance_count = msg.get("instances")
    COMPONENT_INSTANCES.labels("alerter").set(instance_count)

    service_name = msg.get("service_name").lower()
    metrics = msg.get("metrics")
    activity = msg.get("activity")

    busy = activity.get("busy")
    SERVICE_BUSY.labels(service_name).set(busy)

    idle = activity.get("idle")
    SERVICE_IDLE.labels(service_name).set(idle)

    queue = msg.get("queue")
    SERVICE_QUEUE.labels(service_name).set(queue)

    fail_recoverable = metrics.get("fail_recoverable")
    fail_nonrecoverable = metrics.get("fail_nonrecoverable")
    SERVICE_FAILURES.labels(service_name, "recoverable").set(fail_recoverable)
    SERVICE_FAILURES.labels(service_name, "nonrecoverable").set(fail_nonrecoverable)

    execute = metrics.get("execute")
    SERVICE_PROCESSED.labels(service_name).set(execute)


if __name__ == "__main__":
    load_dotenv()
    start_http_server(8000)

    apikey = (os.getenv("ASSEMBLYLINE_USERNAME"), os.getenv("ASSEMBLYLINE_APIKEY"))
    al_client = assemblyline_get_client(os.getenv("ASSEMBLYLINE_HOST"), apikey=apikey)

    # services = al_client.service.list()
    # print(services)

    al_client.socketio.listen_on_status_messages(
        alerter_msg_callback,
        archive_msg_callback,
        dispatcher_msg_callback,
        expiry_msg_callback,
        ingest_msg_callback,
        scaler_msg_callback,
        scaler_status_msg_callback,
        service_msg_callback,
    )
