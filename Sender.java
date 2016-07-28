import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

public class Sender {
	static int srcPort, destPort, seq = 0, ack, windowSize = 1, fragment = 0,
			retrans = 0, bytes = 0;
	static String destIP, logFile, fileName;
	static Map<Integer, DatagramPacket> window = new Hashtable<Integer, DatagramPacket>();
	static Map<Integer, Long> sentTimes = new Hashtable<Integer, Long>();
	static PrintStream logger = System.out;

	static void fillHeader(byte[] buf, int srcPort, int destPort, int seq,
			int ack, int dataOffset, int checkSum, boolean fin) {
		buf[0] = (byte) (srcPort >> 8);
		buf[1] = (byte) (srcPort);

		buf[2] = (byte) (destPort >> 8);
		buf[3] = (byte) (destPort);

		buf[4] = (byte) (seq >> 24);
		buf[5] = (byte) (seq >> 16);
		buf[6] = (byte) (seq >> 8);
		buf[7] = (byte) (seq);

		buf[8] = (byte) (ack >> 24);
		buf[9] = (byte) (ack >> 16);
		buf[10] = (byte) (ack >> 8);
		buf[11] = (byte) (ack);

		buf[12] = (byte) (dataOffset << 4);
		if (fin) {
			buf[13] = 1;
		} else {
			buf[13] = 0;
		}
		buf[14] = 0;
		buf[15] = 0;

		buf[16] = (byte) (checkSum >> 8);
		buf[17] = (byte) (checkSum);

		buf[18] = 0;
		buf[19] = 0;

	}

	static void updateChecksum(byte[] buf) {
		int sum = 0;
		for (int i = 0; i < buf.length; i += 2) {
			if (i == 16) {
				continue;
			}
			sum += buf[i] * 256;
			sum += buf[i + 1];
		}
		if (buf.length % 2 == 1) {
			sum += buf[buf.length - 1] * 256;
			sum += buf[buf.length - 1];
		}
		buf[16] = (byte) (sum >> 8);
		buf[17] = (byte) sum;
	}

	static byte[] createSegment(byte[] data, int len, boolean fin) {
		byte[] buf = new byte[20 + len];
		fillHeader(buf, srcPort, destPort, seq, ack, 20, 0, fin);
		for (int i = 0; i < len; i++) {
			buf[20 + i] = data[i];
		}
		updateChecksum(buf);
		// System.out.printf("send seq=%d ack=%d\n", seq, ack);
		fragment++;
		return buf;
	}

	static DatagramPacket createPacket(byte[] data) throws UnknownHostException {
		DatagramPacket packet = new DatagramPacket(data, data.length,
				InetAddress.getByName(destIP), destPort);
		return packet;
	}

	static void sendWindow(DatagramSocket udp) throws IOException {
		for (DatagramPacket p : window.values()) {
			udp.send(p);
			sentTimes.put(ack, new Date().getTime());
			retrans++;
		}
	}

	static void waitAck() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}
	}

	static void readFile(String name) throws IOException {
		DatagramSocket udp = new DatagramSocket();

		FileInputStream is = new FileInputStream(name);
		byte[] buf = new byte[576];
		while (true) {
			while (window.size() >= windowSize) {
				waitAck();
				sendWindow(udp);
			}
			int len = is.read(buf);
			if (len < 0) {
				while (window.size() > 0) {
					waitAck();
					sendWindow(udp);
				}
				break;
			}

			ack += len;
			DatagramPacket packet = createPacket(createSegment(buf, len, false));
			seq += len;
			udp.send(packet);
			sentTimes.put(ack, new Date().getTime());
			window.put(ack, packet);
			bytes += len;
		}
		is.close();
		ack++;
		DatagramPacket packet = createPacket(createSegment(buf, 0, true));
		udp.send(packet);
		sentTimes.put(ack, new Date().getTime());
		window.put(ack, packet);
		waitAck();
		while (window.size() > 0) {
			waitAck();
			sendWindow(udp);
		}
		udp.close();
	}

	static int getSeq(byte[] buf) {
		int s = (buf[4] & 0xFF) * 0x1000000 + (buf[5] & 0xFF) * 0x10000
				+ (buf[6] & 0xFF) * 0x100 + (buf[7] & 0xFF);
		return s;
	}

	static int getAck(byte[] buf) {
		return (buf[8] & 0xFF) * 0x1000000 + (buf[9] & 0xFF) * 0x10000
				+ (buf[10] & 0xFF) * 0x100 + (buf[11] & 0xFF);
	}

	static void ack(int ack) {
		Date now = new Date();
		DatagramPacket p = window.remove(ack);
		if (p == null) {
			return;
		}
		logger.printf("%s,%d,%d,%d,%d,%d,%dms\n", now, srcPort, destPort,
				getSeq(p.getData()), ack, p.getData()[13], now.getTime()
						- sentTimes.get(ack));
	}

	static void listenTCP(final int port) throws IOException {
		new Thread() {
			@Override
			public void run() {
				ServerSocket server = null;
				try {
					server = new ServerSocket(port);
					Socket soket = server.accept();
					InputStream in = soket.getInputStream();
					ObjectInputStream is = new ObjectInputStream(in);
					while (true) {
						int ack = is.readInt();
						ack(ack);
					}
				} catch (IOException ex) {
				}
				try {
					if (server != null) {
						server.close();
					}
				} catch (IOException e) {
				}
			}
		}.start();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.err
					.println("Usage: sender <filename> <remote_IP> <remote_port> <ack_port_num> <log_filename> <window_size>");
			System.exit(-1);
		}
		int i = 0;
		fileName = args[i++];
		destIP = args[i++];
		destPort = Integer.parseInt(args[i++]);
		 ack port
		srcPort = Integer.parseInt(args[i++]);
		logFile = args[i++];
		if (!logFile.equals("stdout")) {
			logger = new PrintStream(logFile);
		}
		if (args.length > 5) {
			windowSize = Integer.parseInt(args[i++]);
		}

		listenTCP(srcPort);
		readFile(fileName);
		System.out.println("Delivery completed successfully");
		System.out.println("Total bytes sent = " + bytes);
		System.out.println("Segments sent = " + fragment);
		System.out.println("Segments retransmitted = " + retrans);
	}

}
