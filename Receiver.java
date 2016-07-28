import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

public class Receiver {

	static String fileName, senderIP, logFile;
	static int port, senderPort;
	static Socket socket;
	static ObjectOutputStream oos;
	static int current = 0;
	static Map<Integer, byte[]> window = new Hashtable<Integer, byte[]>();
	// static Set<Integer> acks = new HashSet<Integer>();
	static PrintStream logger = System.out;

	static int getSeq(byte[] buf) {
		int s = (buf[4] & 0xFF) * 0x1000000 + (buf[5] & 0xFF) * 0x10000 + (buf[6] & 0xFF) * 0x100 + (buf[7] & 0xFF);
		return s;
	}

	static int getAck(byte[] buf) {
		return (buf[8] & 0xFF) * 0x1000000 + (buf[9] & 0xFF) * 0x10000 + (buf[10] & 0xFF) * 0x100 + (buf[11] & 0xFF);
	}

	static int getSrcPort(byte[] buf) {
		return (buf[0] & 0xFF) * 0x100 + (buf[1] & 0xFF);
	}

	static int getDestPort(byte[] buf) {
		return (buf[2] & 0xFF) * 0x100 + (buf[3] & 0xFF);
	}

	static boolean isFin(byte[] buf) {
		return (buf[13] & 1) == 1;
	}

	static byte[] getData(byte[] buf, int len) {
		byte[] data = new byte[len - 20];
		for (int i = 0; i < data.length; i++) {
			data[i] = buf[i + 20];
		}
		return data;
	}

	static void connectTCP() throws UnknownHostException, IOException {
		socket = new Socket(senderIP, senderPort);
		oos = new ObjectOutputStream(socket.getOutputStream());
	}

	static boolean checksum(byte[] buf, int len) {
		int sum = 0;
		for (int i = 0; i < len; i += 2) {
			if (i == 16) {
				continue;
			}
			sum += buf[i] * 256;
			sum += buf[i + 1];
		}
		if (len % 2 == 1) {
			sum += buf[len - 1] * 256;
			sum += buf[len - 1];
		}
		return buf[16] == ((byte) (sum >> 8)) && buf[17] == (byte) sum;
	}

	static void writeFile(String name) throws IOException {
		FileOutputStream os = new FileOutputStream(name);
		DatagramSocket udp = new DatagramSocket(port);
		byte[] buf = new byte[1024];
		DatagramPacket packet = new DatagramPacket(buf, buf.length);
		while (true) {
			udp.receive(packet);
			Date now = new Date();
			logger.printf("%s,%d,%d,%d,%d,%d\n", now, getSrcPort(packet.getData()), getDestPort(packet.getData()),
					getSeq(packet.getData()), getAck(packet.getData()), packet.getData()[13]);
			if (!checksum(buf, packet.getLength())) {
				continue;
			}
			int seq = getSeq(buf);
			int ack = getAck(buf);
			// sendAck
			if (socket == null) {
				connectTCP();
			}

			oos.writeInt(ack);
			oos.flush();
			if (window.containsKey(seq)) {
				continue;
			}
			// acks.add(ack);
			// System.out.printf("%s,%d,%d,%d,%d,%d\n", new Date(),
			// socket.getLocalPort(), socket.getPort(),
			// System.out.println("ack" + ack);

			if (isFin(buf)) {
				break;
			}
			byte[] data = getData(buf, packet.getLength());
			// System.out.printf("rec seq=%d current=%d ack=%d\n", seq, current,
			// ack);
			if (seq == current) {
				os.write(data);
				current = ack;
			} else {
				// if (!window.containsKey(seq)) {
				window.put(seq, data);
				// }
				while (window.containsKey(current)) {
					byte[] d = window.get(current);
					System.out.println(current);
					os.write(d);
					window.remove(current);
					current += d.length;
				}
			}
		}
		udp.close();
	//	while (!window.isEmpty()) {
			byte[] d = window.get(current);
			os.write(d);
			window.remove(current);
			current += d.length;
		}
		os.close();
		oos.flush();
		// try {
		// Thread.sleep(1000);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		oos.close();
		socket.close();
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println(
					"Usage: java Receiver <filename> <listening_port> <sender_IP> <sender_port> <log_filename>");
			System.exit(-1);
		}
		int i = 0;
		fileName = args[i++];
		port = Integer.parseInt(args[i++]);
		senderIP = args[i++];
		senderPort = Integer.parseInt(args[i++]);
		logFile = args[i++];
		if (!logFile.equals("stdout")) {
			logger = new PrintStream(logFile);
		}
		writeFile(fileName);
		System.out.println("Delivery completed successfully");
	}

}
