param (
    [Parameter(Mandatory=$true, Position=0)]
    [ValidateSet("list", "create", "producer", "consumer", "status")]
    $Action,

    [Parameter(Mandatory=$false, Position=1)]
    $TopicName
)

switch ($Action) {
    "list" {
        docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list
    }
    "create" {
        if (-not $TopicName) { Write-Error "Vui lòng nhập tên topic: .\kafka.ps1 create <topic_name>"; break }
        docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --create --topic $TopicName --partitions 3 --replication-factor 3
    }
    "producer" {
        if (-not $TopicName) { Write-Error "Vui lòng nhập tên topic: .\kafka.ps1 producer <topic_name>"; break }
        docker compose exec broker-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9094 --topic $TopicName
    }
    "consumer" {
        if (-not $TopicName) { Write-Error "Vui lòng nhập tên topic: .\kafka.ps1 consumer <topic_name>"; break }
        docker compose exec broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic $TopicName --from-beginning
    }
    "status" {
        docker compose exec broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9094 describe --status
    }
}
