pools{
    main = {
        backends = {
            "memcached1:11211",
            "memcached2:11211",
        }
    }
}

routes{
    default = route_direct{
        child = "main"
    }
}