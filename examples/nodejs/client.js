/**
 * Aqueduct Node.js Binary Frame Example.
 * Demonstrates binary frame header packing [Magic: 0x1F][Cmd: 1][StreamID: 4][PayloadLen: 4][Payload]
 * for integration with WebTransport / raw UDP QUIC streams.
 */

const MAGIC = 0x1F;
const CMD_PUBLISH = 0x01;
const CMD_SUBSCRIBE = 0x02;

/**
 * Builds a 10-byte binary frame header + payload Buffer.
 * @param {number} cmd - Command byte (1=Publish, 2=Subscribe)
 * @param {number} streamId - 32-bit Stream ID
 * @param {Buffer} payload - Payload Buffer
 * @returns {Buffer} Complete binary frame Buffer
 */
function buildFrame(cmd, streamId, payload) {
    const header = Buffer.alloc(10);
    header.writeUInt8(MAGIC, 0);
    header.writeUInt8(cmd, 1);
    header.writeUInt32LE(streamId, 2);
    header.writeUInt32LE(payload.length, 6);
    return Buffer.concat([header, payload]);
}

/**
 * Parses a 10-byte binary frame header + payload Buffer.
 * @param {Buffer} buffer
 */
function parseFrame(buffer) {
    if (buffer.length < 10) {
        throw new Error("Buffer too short");
    }
    const magic = buffer.readUInt8(0);
    if (magic !== MAGIC) {
        throw new Error(`Invalid magic byte: 0x${magic.toString(16)}`);
    }
    const cmd = buffer.readUInt8(1);
    const streamId = buffer.readUInt32LE(2);
    const payloadLen = buffer.readUInt32LE(6);
    const payload = buffer.subarray(10, 10 + payloadLen);
    return { cmd, streamId, payloadLen, payload };
}

// Demonstration
const subPayload = Buffer.from("topic:orders");
const subFrame = buildFrame(CMD_SUBSCRIBE, 1, subPayload);

console.log("[Node.js Client Example] Serialized 10-Byte Header (Hex):", subFrame.subarray(0, 10).toString("hex"));
console.log("[Node.js Client Example] Parsed Frame:", parseFrame(subFrame));
