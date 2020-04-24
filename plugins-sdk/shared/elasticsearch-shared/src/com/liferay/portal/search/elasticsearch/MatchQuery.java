/**
 * Copyright (c) 2000-present Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.portal.search.elasticsearch;

import com.liferay.portal.kernel.search.BaseQueryImpl;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.index.query.MatchQueryBuilder.ZeroTermsQuery;

/**
 * @author Michael C. Han
 */
public class MatchQuery extends BaseQueryImpl {

	public static final float BOOST_DEFAULT = 1.0f;

	public MatchQuery(String field, String value) {
		_matchQueryBuilder = new MatchQueryBuilder(field, value);
		_field = field;
		_value = value;
	}

	public String getAnalyzer() {
		return _analyzer;
	}

	public float getBoost() {
		return _boost;
	}

	public Float getCutOffFrequency() {
		return _cutOffFrequency;
	}

	public String getField() {
		return _field;
	}

	public Float getFuzziness() {
		return _fuzziness;
	}

	public RewriteMethod getFuzzyRewriteMethod() {
		return _fuzzyRewriteMethod;
	}

	public Integer getMaxExpansions() {
		return _maxExpansions;
	}

	public String getMinShouldMatch() {
		return _minShouldMatch;
	}

	public Operator getOperator() {
		return _operator;
	}

	public Integer getPrefixLength() {
		return _prefixLength;
	}

	public Integer getSlop() {
		return _slop;
	}

	public Type getType() {
		return _type;
	}

	public String getValue() {
		return _value;
	}

	@Override
	public Object getWrappedQuery() {
		return _matchQueryBuilder;
	}

	public ZeroTermsQuery getZeroTermsQuery() {
		return _zeroTermsQuery;
	}

	public Boolean isFuzzyTranspositions() {
		return _fuzzyTranspositions;
	}

	public Boolean isLenient() {
		return _lenient;
	}

	public void setAnalyzer(String analyzer) {
		_matchQueryBuilder.analyzer(analyzer);
		_analyzer = analyzer;
	}

	public void setBoost(float boost) {
		_matchQueryBuilder.boost(boost);
		_boost = boost;
	}

	public void setCutOffFrequency(Float cutOffFrequency) {
		_matchQueryBuilder.cutoffFrequency(cutOffFrequency);
		_cutOffFrequency = cutOffFrequency;
	}

	public void setFuzziness(Float fuzziness) {
		_matchQueryBuilder.fuzziness(fuzziness);
		_fuzziness = fuzziness;
	}

	public void setFuzzyRewriteMethod(RewriteMethod fuzzyRewriteMethod) {
		String fuzzyRewrite = translate(fuzzyRewriteMethod);

		_matchQueryBuilder.fuzzyRewrite(fuzzyRewrite);
		_fuzzyRewriteMethod = fuzzyRewriteMethod;
	}

	public void setFuzzyTranspositions(Boolean fuzzyTranspositions) {
		_matchQueryBuilder.fuzzyTranspositions(fuzzyTranspositions);
		_fuzzyTranspositions = fuzzyTranspositions;
	}

	public void setLenient(Boolean lenient) {
		_matchQueryBuilder.setLenient(lenient);
		_lenient = lenient;
	}

	public void setMaxExpansions(Integer maxExpansions) {
		_matchQueryBuilder.maxExpansions(maxExpansions);
		_maxExpansions = maxExpansions;
	}

	public void setMinShouldMatch(String minShouldMatch) {
		_matchQueryBuilder.minimumShouldMatch(minShouldMatch);
		_minShouldMatch = minShouldMatch;
	}

	public void setOperator(Operator operator) {
		_matchQueryBuilder.operator(operator);
		_operator = operator;
	}

	public void setPrefixLength(Integer prefixLength) {
		_matchQueryBuilder.prefixLength(prefixLength);
		_prefixLength = prefixLength;
	}

	public void setSlop(Integer slop) {
		_matchQueryBuilder.slop(slop);
		_slop = slop;
	}

	public void setType(Type type) {
		_matchQueryBuilder.type(type);
		_type = type;
	}

	public void setZeroTermsQuery(ZeroTermsQuery zeroTermsQuery) {
		_matchQueryBuilder.zeroTermsQuery(zeroTermsQuery);
		_zeroTermsQuery = zeroTermsQuery;
	}

	public enum RewriteMethod {

		CONSTANT_SCORE_AUTO, CONSTANT_SCORE_BOOLEAN, CONSTANT_SCORE_FILTER,
		SCORING_BOOLEAN, TOP_TERMS_N, TOP_TERMS_BOOST_N

	}

	protected String translate(
		MatchQuery.RewriteMethod matchQueryRewriteMethod) {

		if (matchQueryRewriteMethod ==
				MatchQuery.RewriteMethod.CONSTANT_SCORE_AUTO) {

			return "constant_score_auto";
		}
		else if (matchQueryRewriteMethod ==
					MatchQuery.RewriteMethod.CONSTANT_SCORE_BOOLEAN) {

			return "constant_score_boolean";
		}
		else if (matchQueryRewriteMethod ==
					MatchQuery.RewriteMethod.CONSTANT_SCORE_FILTER) {

			return "constant_score_filter";
		}
		else if (matchQueryRewriteMethod ==
					MatchQuery.RewriteMethod.SCORING_BOOLEAN) {

			return "scoring_boolean";
		}
		else if (matchQueryRewriteMethod ==
					MatchQuery.RewriteMethod.TOP_TERMS_N) {

			return "top_terms_N";
		}
		else if (matchQueryRewriteMethod ==
					MatchQuery.RewriteMethod.TOP_TERMS_BOOST_N) {

			return "top_terms_boost_N";
		}

		throw new IllegalArgumentException(
			"Invalid rewrite method: " + matchQueryRewriteMethod);
	}

	private String _analyzer;
	private float _boost = BOOST_DEFAULT;
	private Float _cutOffFrequency;
	private final String _field;
	private Float _fuzziness;
	private RewriteMethod _fuzzyRewriteMethod;
	private Boolean _fuzzyTranspositions;
	private Boolean _lenient;
	private MatchQueryBuilder _matchQueryBuilder;
	private Integer _maxExpansions;
	private String _minShouldMatch;
	private Operator _operator;
	private Integer _prefixLength;
	private Integer _slop;
	private Type _type;
	private final String _value;
	private ZeroTermsQuery _zeroTermsQuery;

}