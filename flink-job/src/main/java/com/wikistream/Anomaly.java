package com.wikistream;

/** A window whose edit count is far above that wiki's recent baseline. */
public record Anomaly(long windowStartMs, String wiki, long edits, double expected, double zScore) {}
