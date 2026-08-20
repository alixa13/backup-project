from http.server import BaseHTTPRequestHandler, HTTPServer

class RequestLoggerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print(f"Request Line: {self.requestline}")
        print("Headers:")
        print(self.headers)  # prints all headers

        # To read body if present (like in POST)
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
            print("Body:")
            print(body.decode())

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

    do_POST = do_GET  # handle POST same way for logging

if __name__ == "__main__":
    server = HTTPServer(('0.0.0.0', 8000), RequestLoggerHandler)
    print("Server running on port 8000")
    server.serve_forever()
