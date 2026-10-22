"""Configuration-based private-network and workload-identity checks."""

from __future__ import annotations

from .common import Context


def run(context: Context):
    phase = "security"

    def no_external_ip():
        value = context.command(
            ["gcloud", "compute", "instances", "describe", "ecommerce-kafka", f"--zone={context.zone}", f"--project={context.project_id}", "--format=value(networkInterfaces[0].accessConfigs[0].natIP)"],
            check=False,
        ).stdout.strip()
        if value:
            raise RuntimeError(f"unexpected external IP: {value}")
        return "Kafka VM has no external IP"

    context.check(phase, "Kafka has no external IP", no_external_ip)

    def firewall_private():
        data = context.json_command(
            ["gcloud", "compute", "firewall-rules", "describe", "ecommerce-kafka-internal", f"--project={context.project_id}", "--format=json"]
        )
        ranges = data.get("sourceRanges", [])
        if "0.0.0.0/0" in ranges or ranges != ["10.20.0.0/24"]:
            raise RuntimeError(f"unexpected sourceRanges: {ranges}")
        return "9092 restricted to 10.20.0.0/24"

    context.check(phase, "Kafka firewall private", firewall_private)

    def dataproc_internal():
        value = context.command(
            ["gcloud", "dataproc", "clusters", "describe", "ecommerce-spark-demo", f"--region={context.region}", f"--project={context.project_id}", "--format=value(config.gceClusterConfig.internalIpOnly)"],
        ).stdout.strip().lower()
        if value != "true":
            raise RuntimeError(f"internalIpOnly={value}")
        return "internalIpOnly=true"

    context.check(phase, "Dataproc internal IP only", dataproc_internal)

    expected_accounts = {"producer-worker", "kafka-broker", "spark-streaming", "ecommerce-dataset"}
    accounts = set(
        context.command(
            ["gcloud", "iam", "service-accounts", "list", f"--project={context.project_id}", "--format=value(email)"],
        ).stdout.splitlines()
    )
    missing = [name for name in expected_accounts if not any(email.startswith(f"{name}@") for email in accounts)]
    context.add(phase, "separate workload identities", "FAIL" if missing else "PASS", f"missing={missing}" if missing else ", ".join(sorted(expected_accounts)))

    keys = []
    for name in expected_accounts:
        result = context.command(
            ["gcloud", "iam", "service-accounts", "keys", "list", f"--iam-account={name}@{context.project_id}.iam.gserviceaccount.com", f"--project={context.project_id}", "--managed-by=user", "--format=value(name)"],
            check=False,
        )
        keys.extend(result.stdout.splitlines())
    context.add(phase, "no user-managed service-account keys", "FAIL" if keys else "PASS", f"keys={keys}" if keys else "none")
