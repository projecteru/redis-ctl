function sortNodeByAddr(a, b) {
    if (a.host == b.host) {
        return a.port - b.port;
    }
    return a.host < b.host ? -1 : 1;
}
