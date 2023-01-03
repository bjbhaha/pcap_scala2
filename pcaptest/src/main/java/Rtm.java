import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.io.Serializable;
import java.util.Map;

public class Rtm implements Serializable {
//public class Rtm implements Comparable<Datagram> {
    private String src;
    private String dst;
    private Integer src_port;
    private Integer dst_port;
    private Long tcp_seq;
    private Integer len;

    public Rtm(String src, String dst, Integer src_port, Integer dst_port, Long tcp_seq, Integer len) {
        this.src = src;
        this.dst = dst;
        this.src_port = src_port;
        this.dst_port = dst_port;
        this.tcp_seq = tcp_seq;
        this.len = len;
    }

    //@Override
    public int compareTo(Rtm o) {
        return ComparisonChain.start()
                .compare(src, o.src, Ordering.natural().nullsLast())
                .compare(dst, o.dst, Ordering.natural().nullsLast())
                .compare(src_port, o.src_port, Ordering.natural().nullsLast())
                .compare(dst_port, o.dst_port, Ordering.natural().nullsLast())
                .compare(tcp_seq, o.tcp_seq, Ordering.natural().nullsLast())
                .compare(len, o.len, Ordering.natural().nullsLast())
                .result();
    }

    public boolean equals(Rtm o) {
        return java.util.Objects.equals(src, o.src) &&
                java.util.Objects.equals(dst, o.dst) &&
                java.util.Objects.equals(src_port, o.src_port) &&
                java.util.Objects.equals(dst_port, o.dst_port) &&
                java.util.Objects.equals(tcp_seq, o.tcp_seq) &&
                java.util.Objects.equals(len, o.len);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass()).add("src", src)
                .add("dst", dst)
                .add("src_port", src_port)
                .add("dst_port", dst_port)
                .add("tcp_seq", tcp_seq)
                .add("len", len)
                .toString();
    }

}