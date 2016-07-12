package de.dailab.newsreel.recommender.common.extimpl.unit.util;

import de.dailab.newsreel.recommender.common.util.UpdateableTreeSet;

import java.util.Map;
import java.util.TreeSet;



/**
 * This is a simple demonstration class for testing {@link TreeSet} and {@link UpdateableTreeSet}.
 * <p>
 * Thus, for simplicity's sake methods like {@code hashCode}, {@code equals} and {@code compareTo} assume that members
 * and objects are always cleanly initialised (no null or negative values) and have the expected types.
 */
class MedalCount implements Comparable<MedalCount>, UpdateableTreeSet.Updateable {
	String country;
	int gold;
	int silver;
	int bronze;

	MedalCount(String country) {
		this.country = country;
	}

	MedalCount(String country, int gold, int silver, int bronze) {
		this(country);
		this.gold = gold;
		this.silver = silver;
		this.bronze = bronze;
	}

	@Override
	public int hashCode() {
		return country.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		MedalCount other = (MedalCount) obj;
		// Equality is based on country name because there can be only one MedalCount per country
		return country.equals(other.country);
	}

	@Override
	public int compareTo(MedalCount other) {
		// Be consistent with equals as recommended by Comparable
		if (this.equals(other))
			return 0;
		// More gold medals -> higher ranking -> "less than". Etc.
		if (gold > other.gold)
			return -1;
		else if (gold < other.gold)
			return 1;
		else if (silver > other.silver)
			return -1;
		else if (silver < other.silver)
			return 1;
		else if (bronze > other.bronze)
			return -1;
		else if (bronze < other.bronze)
			return 1;
		// Different countries with equal medal stats should not be regarded as equal here because otherwise only one
		// of them would be allowed in a TreeSet. Thus, order countries alphabetically if medal stats are equal.
		return this.country.compareTo(other.country);
	}

	@Override
	public void update() { }

	@Override @SuppressWarnings("unchecked")
	public void update(Object newValue) {
		if (newValue == null)
			return;
		Map<String, Object> values = (Map<String, Object>) newValue;
		for (String key : values.keySet()) {
			Object value = values.get(key);
			if ("country".equals(key))
				country = (String) value;
			else if ("gold".equals(key))
				gold = (Integer) value;
			else if ("silver".equals(key))
				silver = (Integer) value;
			else if ("bronze".equals(key))
				bronze = (Integer) value;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MedalCount [country=").append(country).append(", gold=").append(gold).append(", silver=")
			.append(silver).append(", bronze=").append(bronze).append("]");
		return builder.toString();
	}
}