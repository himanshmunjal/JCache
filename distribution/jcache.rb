# Homebrew formula for JCache. The release workflow copies this file to the
# himanshmunjal/homebrew-jcache tap and fills in the version and checksums.
class Jcache < Formula
  desc "In-memory cache server with LRU, LFU and ARC eviction"
  homepage "https://github.com/himanshmunjal/JCache"
  url "https://github.com/himanshmunjal/JCache/releases/download/v1.0.3/jcache-server-1.0.3.jar"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "MIT"

  depends_on "openjdk@21"

  resource "client" do
    url "https://github.com/himanshmunjal/JCache/releases/download/v1.0.3/jcache-client-1.0.3.jar"
    sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  end

  def install
    libexec.install "jcache-server-#{version}.jar"
    resource("client").stage { libexec.install "jcache-client-#{version}.jar" }

    java = Formula["openjdk@21"].opt_bin/"java"
    (bin/"jcache-server").write <<~SH
      #!/bin/bash
      exec "#{java}" ${JAVA_OPTS:--Xmx512m} -jar "#{libexec}/jcache-server-#{version}.jar" "$@"
    SH
    (bin/"jcache").write <<~SH
      #!/bin/bash
      exec "#{java}" -jar "#{libexec}/jcache-client-#{version}.jar" "$@"
    SH

    (etc/"jcache").mkpath
    unless (etc/"jcache/jcache.properties").exist?
      (etc/"jcache/jcache.properties").write <<~PROPS
        server.port=6379
        cache.capacity=10000
        cache.policy=LRU
        persistence.enabled=true
        persistence.snapshot.path=#{var}/jcache
      PROPS
    end
  end

  service do
    run [opt_bin/"jcache-server", "--config", etc/"jcache/jcache.properties"]
    keep_alive true
    log_path var/"log/jcache.log"
    error_log_path var/"log/jcache.log"
  end

  test do
    port = free_port
    pid = spawn bin/"jcache-server", "--port", port.to_s
    begin
      sleep 3
      TCPSocket.open("127.0.0.1", port) do |sock|
        sock.write "PUT greeting hello world\r\n"
        assert_equal "+OK", sock.gets.chomp
        sock.write "GET greeting\r\n"
        assert_equal "+hello world", sock.gets.chomp
      end
    ensure
      Process.kill("TERM", pid)
      Process.wait(pid)
    end
  end
end
