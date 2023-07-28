package main

import (
	"net/http"

	"github.com/rs/zerolog/log"
)

func HTTPLogWrapper(original_handler http.Handler) http.Handler {
	logFn := func(rw http.ResponseWriter, r *http.Request) {
		uri := r.RequestURI
		method := r.Method
		peer := r.RemoteAddr

		// Serve the request
		original_handler.ServeHTTP(rw, r)

		// Log the details
		log.Info().Str("uri", uri).Str("method", method).Str("peer", peer).Msg("HTTP Request")
	}
	return http.HandlerFunc(logFn)
}
