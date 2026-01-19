import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * DFSMaster
 *
 * コメント多めの説明付きマスターサーバ実装。
 * - 子サーバは REGISTER host port path で登録する
 * - 子サーバは定期的に KEEP_ALIVE host port path を送ることで生存を知らせる
 * - クライアントは GET filename でファイルパスにマッチする子サーバ(host:port)を取得する
 * - LIST で現在の登録一覧を取得できる
 *
 * 実装上の注意:
 * - servers は path(prefix) をキーに ServerEntry を保存する（最長一致で選択）
 * - タイムアウトで古い登録を自動削除する startTimeoutCleaner を持つ
 * - シンプルなテキストプロトコル（改良の余地あり）
 */
public class DFSMaster {
    private static final int PORT = 8080;

    // path prefix -> ServerEntry のマップ（スレッドセーフ）
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

                    // スペースで分割（最大4つ）: コマンドと引数を安全に解析
                    String[] parts = line.split(" ", 4);
                    String cmd = parts.length > 0 ? parts[0] : "";
                    System.out.println("[Master] Received from " + clientId + ": " + line);

                    switch (cmd) {
                        case "REGISTER":
                            handleRegister(parts, out);
                            break;

                        case "OPEN":
                            handleOpen(parts, out);
                            break;

                        case "CLOSE":
                            // 特に処理はなし
                            out.println("OK");
                            break;
                        
                        default:
                            out.println("ERROR Unknown command");
                    }
                }
            } catch (IOException e) {
                // 接続異常時はログだけ出して終了
                System.err.println("[Master] IO error with client " + clientId + ": " + e.getMessage());
            } 
        }

        /**
         * handleRegister:
         * - parts: ["REGISTER", host, port, path]
         * - 登録要求に対して servers にエントリを追加する
         * - 同一 path が既にあれば上書きする
         */

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
            if (parts.length < 4) {
                out.println("ERROR usage: REGISTER host port path");
                return;
            }

            String host = parts[1];
            int port;
            try {
                port = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                out.println("ERROR invalid port");
                return;
            }

            String path = distributeServerPath(parts[3]);   // パス名が./A,./B,...と登録されるようにする

            servers.put(path, new ServerEntry(host, port, path));
            out.println("OK");
            System.out.println("[Master] REGISTER " + path + " @ " + host + ":" + port);
        }

    
        private void handleOpen(String[] parts, PrintWriter out) {
            if (parts.length < 3) {
                out.println("ERROR usage: Open filename mode");
                return;
            }
            
            String filePath = parts[1];

            for (String path : servers.keySet()) {//クライアントのパスにマッチするサーバを探す
                //System.out.println("Checking path: " + path);
                if (filePath.startsWith(path)) { 
                    ServerEntry server = servers.get(path);
                    out.println("OK " + server.port);//マッチしたサーバのポート番号を返す
                }
            }
            out.println("ERROR No matching server for " + filePath);

        }

    }
}