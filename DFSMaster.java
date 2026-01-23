import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DFSMaster {
    private static final int PORT = 8080;

    // path prefix -> ServerEntry のマップ
    private static final Map<String, ServerEntry> servers = new ConcurrentHashMap<>();
    
  
    static class ServerEntry {//子サーバ情報を管理するクラス
        final String host;
        final int port;
        final String path;

        ServerEntry(String host, int port, String path) {
            this.host = host;
            this.port = port;
            this.path = path;
        }

    }

    public static void main(String[] args) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("DFSMaster started on port " + PORT);
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new ClientHandler(clientSocket).start();
            }
        }
    }


    static class ClientHandler extends Thread {
        private final Socket socket;
        private final String clientId;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.clientId = socket.getRemoteSocketAddress().toString();
        }

        @Override
        public void run() {
            
             try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                
                String line;
                // クライアントからの各行を処理するループ
                while ((line = in.readLine()) != null) {
                    if (line.isEmpty()) continue;

                    String[] parts = line.split(" ", 4);
                    String cmd = parts[0];
                    System.out.println("Received from " + clientId + ": " + line);

                    switch (cmd) {
                        case "REGISTER":
                            handleRegister(parts, out);
                            break;

                        case "OPEN":
                            handleOpen(parts, out);
                            break;

                        case "CLOSE":
                            out.println("OK");
                            break;
                        
                        case "LIST":
                            handleList(out);
                            break;
                        
                        default:
                            out.println("[ERROR] Unknown command");
                    }
                }
            } catch (IOException e) {
                System.err.println("[ERROR] IO error with client " + clientId + ":" + e.getMessage());
            } 
        }

        String distributeServerPath(String basePath) {
            // 子サーバにpath を ./A, ./B, ./C,... と分配する
            char offset = 'A';
            String path;

            do {
                path = basePath + offset;
                offset++;
            } while (servers.containsKey(path));
            return path;
        }

        private void handleRegister(String[] parts, PrintWriter out) {

            String host = parts[1];
            int port;
            try {
                port = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                out.println("[ERROR] invalid port");
                return;
            }

            String path = distributeServerPath(parts[3]);   // パス名が./A,./B,...と登録されるようにする

            servers.put(path, new ServerEntry(host, port, path));
            out.println("OK");
            System.out.println("Registered " + path + " @ " + host + ":" + port);
        }

    
        private void handleOpen(String[] parts, PrintWriter out) {
            if (parts.length < 3) {
                out.println("[ERROR] format: <Open> <filepath> <mode>");
                return;
            }

            
            String filePath = parts[1];

            for (String path : servers.keySet()) {//クライアントのパスにマッチするサーバを探す
                //System.out.println("Checking path: " + path);
                if (filePath.startsWith(path)) { 
                    ServerEntry server = servers.get(path);
                    out.println("OK " + server.port);//マッチしたサーバのポート番号を返す
                    return;
                }

            }
            out.println("[ERROR] No matching server for " + filePath);

        }

        private void handleList(PrintWriter out) {
            out.println("FILES_LIST_START");
            for (String path : servers.keySet()) {//登録サーバのパスを走査して
                ServerEntry server = servers.get(path);//各子サーバー情報のインスタンスを取得

                try (Socket child = new Socket(server.host, server.port);
                    BufferedReader inChild = new BufferedReader(new InputStreamReader(child.getInputStream()));
                    PrintWriter outChild = new PrintWriter(child.getOutputStream(), true)   ) {
                    //子サーバとの通信を開始
                    outChild.println("LIST");
                    out.println("SERVER: " + server.path);
                    String childList;
                    while ((childList = inChild.readLine()) != null) {
                        if ("FILES_LIST_START".equals(childList)) continue;//開始の合図
                        if ("FILES_LIST_END".equals(childList)) break;//終了の合図
                        
                        out.println(childList);//子サーバからの応答
                    }
                
                } catch (IOException e) {
                    out.println("[ERROR] listing from " + server.path + ": " + e.getMessage());
                }
            }
            
            out.println("FILES_LIST_END");
        }
    }
}