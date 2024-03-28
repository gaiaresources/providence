<?php
/** ---------------------------------------------------------------------
 * app/lib/Plugins/SearchEngine/Elastic8/FieldTypes/FieldType.php :
 * ----------------------------------------------------------------------
 * CollectiveAccess
 * Open-source collections management software
 * ----------------------------------------------------------------------
 *
 * Software by Whirl-i-Gig (http://www.whirl-i-gig.com)
 * Copyright 2015 Whirl-i-Gig
 *
 * For more information visit http://www.CollectiveAccess.org
 *
 * This program is free software; you may redistribute it and/or modify it under
 * the terms of the provided license as published by Whirl-i-Gig
 *
 * CollectiveAccess is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTIES whatsoever, including any implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * This source code is free and modifiable under the terms of
 * GNU General Public License. (http://www.gnu.org/copyleft/gpl.html). See
 * the "license.txt" file for details, or visit the CollectiveAccess web site at
 * http://www.CollectiveAccess.org
 *
 * @package CollectiveAccess
 * @subpackage Search
 * @license http://www.gnu.org/copyleft/gpl.html GNU Public License version 3
 *
 * ----------------------------------------------------------------------
 */

namespace Elastic8\FieldTypes;

use ca_metadata_elements;
use Datamodel;
use DateTime;
use MemoryCacheInvalidParameterException;
use Zend_Search_Lucene_Index_Term;

abstract class FieldType {
	protected const SUFFIX_TEXT = 's';
	protected const SUFFIX_IDNO = 'idno';
	protected const SUFFIX_TOKENIZE_WS = 'tokenize-ws';
	protected const SUFFIX_KEYWORD = 'kw';
	protected const SUFFIX_INTEGER = 'i';
	protected const SUFFIX_FLOAT = 'f';
	protected const SUFFIX_DOUBLE = 'd';
	protected const SUFFIX_BOOLEAN = 'b';
	protected const SUFFIX_LONG = 'l';
	protected const SUFFIX_LONG_RANGE = 'lr';
	protected const SUFFIX_INTEGER_RANGE = 'ir';
	protected const SUFFIX_DOUBLE_RANGE = 'dr';
	protected const SUFFIX_WILDCARD = 'w';
	protected const SUFFIX_GEO_SHAPE = 'gs';
	protected const SUFFIX_GEO_POINT = 'gp';
	protected const SUFFIX_OBJECT = 'o';
	protected const SUFFIX_DATE = 'dt';
	protected const SUFFIX_DATE_RANGE = 'dtr';
	protected const SUFFIX_TIME = 't';
	protected const SUFFIX_TIME_RANGE = 'tr';
	protected const SUFFIX_CURRENCY = 'currency';
	protected const SUFFIX_TIMESTAMP = 'ts';
	protected const SUFFIX_SEPARATOR = '-';

	/**
	 * @param mixed $content
	 */
	abstract public function getIndexingFragment($content, array $options): array;

	abstract public function getRewrittenTerm(Zend_Search_Lucene_Index_Term $term): ?Zend_Search_Lucene_Index_Term;

	protected static function getLogger() {
		static $logger;
		if ($logger === null) {
			$logger = caGetLogger();
		}

		return $logger;
	}

	/**
	 * Allows implementations to add additional terms to the query
	 *
	 * @return bool|array
	 */
	public function getAdditionalTerms(Zend_Search_Lucene_Index_Term $term) {
		return false;
	}

	/**
	 * Allows implementations to add ElasticSearch query filters
	 */
	public function getQueryFilters(Zend_Search_Lucene_Index_Term $term): bool {
		return false;
	}

	abstract public function getKey(): string;

	/**
	 * @throws MemoryCacheInvalidParameterException
	 */
	public static function getInstance(string $table, string $content_fieldname): ?FieldType {
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/DateRange.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Geocode.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Currency.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Length.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/ListItem.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Weight.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Timecode.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Integer.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Numeric.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/GenericElement.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/Intrinsic.php');
		require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/ChangeLogDate.php');

		// if this is an indexing field name, rewrite it
		$could_be_attribute = true;
		if (preg_match("/^([IA])[0-9]+$/", $content_fieldname)) {

			if ($content_fieldname[0] === 'A') { // Metadata attribute
				$field_num_proc = (int) substr($content_fieldname, 1);
				$content_fieldname = ca_metadata_elements::getElementCodeForId($field_num_proc);
				if (!$content_fieldname) {
					return null;
				}
			} else {
				// Plain intrinsic
				$could_be_attribute = false;
				$field_num_proc = (int) substr($content_fieldname, 1);
				$content_fieldname = Datamodel::getFieldName($table, $field_num_proc);
			}

		}

		if ($content_fieldname && $could_be_attribute) {
			$tmp = explode('/', $content_fieldname);
			$content_fieldname = array_pop($tmp);
			$datatype = ca_metadata_elements::getElementDatatype($content_fieldname);
			if ($datatype !== null) {
				switch ($datatype) {
					case __CA_ATTRIBUTE_VALUE_DATERANGE__:
						return new DateRange($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_GEOCODE__:
						return new Geocode($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_CURRENCY__:
						return new Currency($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_LENGTH__:
						return new Length($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_WEIGHT__:
						return new Weight($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_TIMECODE__:
						return new Timecode($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_INTEGER__:
						return new Integer($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_NUMERIC__:
						return new Numeric($table, $content_fieldname);
					case __CA_ATTRIBUTE_VALUE_LIST__:
						return new ListItem($table, $content_fieldname);
					default:
						return new GenericElement($table, $content_fieldname);
				}
			}
		}

		return new Intrinsic($table, $content_fieldname);
	}

	public function getDataTypeSuffix($suffix = null): string {
		if (is_null($suffix)) {
			$suffix = $this->getDefaultSuffix();
		}

		return $this->getSeparator() . $suffix;
	}

	protected function getSeparator(): string {
		return self::SUFFIX_SEPARATOR;
	}

	/**
	 * @param $content
	 *
	 * @return string|int|float|bool
	 * @deprecated TODO: This serialize call existed in the legacy codebase. Let's run a full reindex and confirm that
	 *     this doesn't happen. If so then we can remove this method.
	 */
	public function serializeIfArray($content) {
		if (is_array($content)) {
			self::getLogger()->logError(_t('Unexpected data type for content %s', json_encode($content)));
			$content = serialize($content);
		}

		return $content;
	}

	/**
	 * @param $content
	 *
	 * @return array|array[]
	 */
	public function parseElasticsearchDateRange($content, $historic = false): array {
		$return = [];
		$return[$this->getDataTypeSuffix()] = $content;
		if ($historic) {
			$dates = explode(' - ', $content);
			$dates = array_map('trim', $dates);
			$dates = array_map(function ($date) {
				$timestamp_date = (new DateTime())->setTimestamp(caHistoricTimestampToUnixTimestamp($date));
				$min_date = (new DateTime('-9999-01-01'))->getTimestamp();
				$max_date = (new DateTime('9999-12-31'))->getTimestamp();
				/** @var DateTime $actual_date */
				$actual_date = max($min_date, min($timestamp_date, $max_date)); // Return the modified timestamp

				return $actual_date->format("c");
			}, $dates);

			$rewritten_start = $dates[0] ?? null;
			$rewritten_end = $dates[1] ?? $rewritten_start;
		} else {
			$parsed_content = caGetISODates($content, ['returnUnbounded' => true]);
			$rewritten_start = caRewriteDateForElasticSearch($parsed_content["start"], true);
			$rewritten_end = caRewriteDateForElasticSearch($parsed_content["end"], false);
		}
		if (!($rewritten_start) && !($rewritten_end)) {
			return $return;
		}

		$return[$this->getDataTypeSuffix(FieldType::SUFFIX_DATE_RANGE)] = [
			'gte' => $rewritten_start,
			'lte' => $rewritten_end
		];

		return [$this->getKey() => $return];
	}

	/**
	 * @return string
	 */
	public function getDefaultSuffix(): string {
		return self::SUFFIX_TEXT;
	}

}
