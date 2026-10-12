package com.wikistream;

/** Edit counts for one wiki in one tumbling window. */
public record WindowStats(long windowStartMs, String wiki, long edits, long botEdits) {}
