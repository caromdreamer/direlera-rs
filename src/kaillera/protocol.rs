use std::cmp;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub message_number: u16,
    #[allow(dead_code)]
    pub message_length: u16,
    pub message_type: u8,
    pub data: Vec<u8>,
}

pub fn make_packet(message_type: u8, seq: u16, data: Vec<u8>) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&seq.to_le_bytes());
    packet.extend_from_slice(&(data.len() as u16 + 1).to_le_bytes());
    packet.extend_from_slice(&message_type.to_le_bytes());
    packet.extend_from_slice(&data);
    packet
}

/// Generates UDP packets with reliability: sends the last N packets together
/// to handle packet loss. Each transmission includes the newest packet plus
/// up to 2 previous packets for redundancy.
#[derive(Debug, Clone)]
pub struct UDPPacketGenerator {
    recent_packets: VecDeque<Vec<u8>>,
    send_count: u16,
    /// Exact bytes of the last packet sent to this client (redundancy bundle
    /// included). Resent verbatim to a stalled client; message numbers are
    /// unchanged so the client dedups already-processed messages.
    last_sent: Option<Vec<u8>>,
}

impl Default for UDPPacketGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl UDPPacketGenerator {
    const MAX_RECENT_PACKETS: usize = 3;

    pub fn new() -> Self {
        Self {
            recent_packets: VecDeque::with_capacity(Self::MAX_RECENT_PACKETS),
            send_count: 0,
            last_sent: None,
        }
    }

    /// Creates a new packet and bundles it with recent packets for reliability.
    /// Returns: [packet_count][newest_packet][second_newest][oldest]
    pub fn make_send_packet(&mut self, message_type: u8, data: Vec<u8>) -> Vec<u8> {
        // Create and store new packet
        let new_packet = make_packet(message_type, self.send_count, data);

        // Maintain only last N packets in memory
        if self.recent_packets.len() >= Self::MAX_RECENT_PACKETS {
            self.recent_packets.pop_front();
        }
        self.recent_packets.push_back(new_packet);

        // Build final packet with redundancy
        let packet_count = cmp::min(Self::MAX_RECENT_PACKETS, self.recent_packets.len());
        let mut result = Vec::with_capacity(1 + packet_count * 128);
        result.push(packet_count as u8);

        // Add packets in reverse order (newest first)
        for packet in self.recent_packets.iter().rev() {
            result.extend_from_slice(packet);
        }

        self.send_count = self.send_count.wrapping_add(1);
        self.last_sent = Some(result.clone());
        result
    }

    /// Returns the exact bytes of the last packet sent to this client, for
    /// resending when the client stalls (stops sending game input). Returns
    /// `None` if nothing has been sent yet.
    pub fn last_sent(&self) -> Option<Vec<u8>> {
        self.last_sent.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_packet() {
        let sample_data = vec![1u8, 2, 3, 4];
        let packet = make_packet(0x10, 2, sample_data);
        assert_eq!(packet, vec![2, 0, 5, 0, 0x10, 1, 2, 3, 4]);
    }

    #[test]
    fn test_udp_generator() {
        let sample_data = vec![1u8, 2, 3, 4];
        let mut udp_generator = UDPPacketGenerator::new();
        let result = udp_generator.make_send_packet(0x10, sample_data.clone());
        assert_eq!(
            result,
            vec![
                1, // messages
                0, 0, // seq
                5, 0, // length
                0x10, 1, 2, 3, 4
            ]
        );
        let result = udp_generator.make_send_packet(0x11, sample_data.clone());
        assert_eq!(
            result,
            vec![
                2, // messages
                1, 0, // seq
                5, 0, // length
                0x11, 1, 2, 3, 4, // message type, data
                0, 0, // seq
                5, 0, // length
                0x10, 1, 2, 3, 4, // message type, data
            ]
        );
        let result = udp_generator.make_send_packet(0x12, sample_data.clone());
        assert_eq!(
            result,
            vec![
                3, // messages
                2, 0, // seq
                5, 0, // length
                0x12, 1, 2, 3, 4, // message type, data
                1, 0, // seq
                5, 0, // length
                0x11, 1, 2, 3, 4, // message type, data
                0, 0, // seq
                5, 0, // length
                0x10, 1, 2, 3, 4, // message type, data
            ]
        );
        let result = udp_generator.make_send_packet(0x13, sample_data.clone());
        assert_eq!(
            result,
            vec![
                3, // messages
                3, 0, // seq
                5, 0, // length
                0x13, 1, 2, 3, 4, // message type, data
                2, 0, // seq
                5, 0, // length
                0x12, 1, 2, 3, 4, // message type, data
                1, 0, // seq
                5, 0, // length
                0x11, 1, 2, 3, 4, // message type, data
            ]
        );
    }
}
