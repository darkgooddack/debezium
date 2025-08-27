import asyncio
import json
from aiokafka import AIOKafkaConsumer


async def consume():
    consumer = AIOKafkaConsumer(
        "dbserver1.inventory.products",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="products-consumer-group"
    )

    await consumer.start()
    try:
        async for msg in consumer:
            event = msg.value
            print("Событие:", json.dumps(event, indent=2, ensure_ascii=False))
            if event.get("op") == "c":
                product = event["after"]
                print("Продукт:", product)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
