package com.wikistream;

/** One edit (or page creation) parsed from a Wikimedia recentchange event. */
public record WikiEdit(String wiki, boolean bot, long timestampMs) {}
