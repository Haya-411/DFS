import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DFSServer {
    private static final int PORT = 8080;
    // ファイル内容をメモリ上で管理
    private static Map<String, String> fileStore = new ConcurrentHashMap<>();
    // 書き込み制限(同時書き込みを防ぐ)
    private static Map<String, String> writeLocks = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("DFS Server started on port " + PORT);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(new ClientHandler(clientSocket)).start();
        }
    }

    private static class ClientHandler implements Runnable {
        private Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                String line = in.readLine();
                if (line == null)
                    return;
                String[] parts = line.split(" ", 3);
                String command = parts[0];

                if ("FETCH".equals(command)) {
                    String path = parts[1];
                    String mode = parts[2];

                    // 書き込みモード -> ロック管理
                    if (mode.contains("WRITE")) {
                        if (writeLocks.containsKey(path)) {
                            out.println("ERROR: File is locked by another client");
                            return;
                        }
                        writeLocks.put(path, socket.getRemoteSocketAddress().toString());
                    }

                    String content = fileStore.getOrDefault(path, "");
                    out.println("SUCCESS");
                    out.println(content); // 実際は終端文字などの工夫が必要

                } else if ("STORE".equals(command)) {
                    String path = parts[1];
                    StringBuilder content = new StringBuilder();
                    String inputLine;
                    while (!(inputLine = in.readLine()).equals("__END__")) {
                        content.append(inputLine).append("\n");
                    }
                    fileStore.put(path, content.toString().trim());
                    writeLocks.remove(path); // 更新後にロック解除
                    out.println("SUCCESS");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
