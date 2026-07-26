"""
Aqueduct Python QUIC Client Example.
Demonstrates binary frame construction [Magic: 0x41][Cmd][StreamID][PayloadLen][Payload]
and asynchronous stream processing using aioquic.
"""

import asyncio
import struct
from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration

MAGIC = 0x41  # 'A' ASCII byte
CMD_PUBLISH = 0x01
CMD_SUBSCRIBE = 0x02


def build_frame(cmd: int, stream_id: int, payload: bytes) -> bytes:
    """Build 10-byte binary header + payload."""
    header = struct.pack("!BBII", MAGIC, cmd, stream_id, len(payload))
    return header + payload


def parse_frame(data: bytes):
    """Parse 10-byte binary header + payload."""
    if len(data) < 10:
        raise ValueError("Buffer too short")
    magic, cmd, stream_id, payload_len = struct.unpack("!BBII", data[:10])
    if magic != MAGIC:
        raise ValueError(f"Invalid magic byte: {hex(magic)}")
    payload = data[10 : 10 + payload_len]
    return cmd, stream_id, payload


async def main():
    config = QuicConfiguration(is_client=True, alpn_protocols=["aqueduct-v1"])
    config.verify_mode = False  # Skip verification for self-signed dev cert

    print("[Python Client] Connecting to Aqueduct QUIC broker on 127.0.0.1:4242...")

    async with connect("127.0.0.1", 4242, configuration=config) as client:
        # 1. Open Stream and Subscribe
        reader, writer = await client.create_stream()
        sub_frame = build_frame(CMD_SUBSCRIBE, 1, b"topic:orders")
        writer.write(sub_frame)
        print("[Python Client] Sent Subscribe frame for topic 'orders'")

        await asyncio.sleep(0.2)

        # 2. Open Stream and Publish
        pub_reader, pub_writer = await client.create_stream()
        pub_frame = build_frame(CMD_PUBLISH, 2, b"orders")
        pub_writer.write(pub_frame)
        print("[Python Client] Sent Publish frame to topic 'orders'")

        # 3. Read delivered message
        data = await reader.read(1024)
        cmd, stream_id, payload = parse_frame(data)
        print(f"[Python Client] Received frame: Cmd={cmd}, StreamID={stream_id}, Payload={payload.decode()}")


if __name__ == "__main__":
    asyncio.run(main())
