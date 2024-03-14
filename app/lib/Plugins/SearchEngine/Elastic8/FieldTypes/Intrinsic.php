<?php
/** ---------------------------------------------------------------------
 * app/lib/Plugins/SearchEngine/Elastic8/FieldTypes/Intrinsic.php :
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

use BaseLabel;
use BaseModel;
use Datamodel;
use Zend_Search_Lucene_Index_Term;

require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/FieldTypes/FieldType.php');

class Intrinsic extends FieldType {

	/**
	 * Table name
	 */
	protected string $table_name;
	/**
	 * Field name
	 */
	protected string $field_name;

	/**
	 * Intrinsic constructor.
	 */
	public function __construct(string $table_name, string $field_name) {
		$this->table_name = $table_name;
		$this->field_name = $field_name;
	}

	public function getTableName(): string {
		return $this->table_name;
	}

	public function getFieldName(): string {
		return $this->field_name;
	}

	/**
	 * @param mixed $content
	 */
	public function getIndexingFragment($content, array $options): array {
		$content = $this->serializeIfArray($content);
		if ($content === '') {
			$content = null;
		}
		$fieldTypeToSuffix = [
			FT_BIT => self::SUFFIX_BOOLEAN,
			FT_TIME => self::SUFFIX_TIME,
			FT_NUMBER => self::SUFFIX_INTEGER,// TODO: Are there any FT_NUMBER which are not integer,
			FT_TIMERANGE => self::SUFFIX_TIME_RANGE,
			FT_TIMECODE => self::SUFFIX_TIME,
		];

		$instance = Datamodel::getInstance($this->getTableName(), true);
		$field_info = Datamodel::getFieldInfo($this->getTableName(), $this->getFieldName());

		$fieldType = $field_info['FIELD_TYPE'];
		$suffix = $fieldTypeToSuffix[$fieldType] ?? self::SUFFIX_TEXT;
		switch ($fieldType) {
			case (FT_BIT):
				$content = (bool) $content;
				break;
			case (FT_NUMBER):
				if (in_array($this->getFieldName(), ['hier_left', 'hier_right'])) {
					$suffix = self::SUFFIX_DOUBLE;
				}
			case (FT_TIME):
			case (FT_TIMERANGE):
			case (FT_TIMECODE):
				if (!isset($field_info['LIST_CODE'])) {
					$content = (float) $content;
				} else if ($suffix !== self::SUFFIX_DOUBLE) {
					$suffix = self::SUFFIX_KEYWORD;
				}
				break;
			default:
				// noop (pm_content is just pm_content)
				break;
		}

		if (in_array('DONT_TOKENIZE', $options, true)) {
			$suffix = self::SUFFIX_KEYWORD;
		}


		if ($instance->getProperty('ID_NUMBERING_ID_FIELD') == $this->getFieldName()
			|| in_array('INDEX_AS_IDNO', $options, true)
		) {
			if (method_exists($instance, "getIDNoPlugInInstance")
				&& ($idno
					= $instance->getIDNoPlugInInstance())
			) {
				$values = array_values($idno->getIndexValues($content));
			} else {
				$values = explode(' ', $content);
			}

			$return = [
				$this->getDataTypeSuffix(self::SUFFIX_IDNO) => $values
			];
		} else {
			$return = [
				$this->getDataTypeSuffix($suffix) => $content
			];
		}

		if ($rel_type_id = caGetOption('relationship_type_id', $options)) {
			$return[caGetRelationshipTypeCode($rel_type_id) . $this->getDataTypeSuffix(self::SUFFIX_KEYWORD)]
				= $content;
		}

		return [$this->getKey() => $return];
	}

	public function getRewrittenTerm(Zend_Search_Lucene_Index_Term $term): Zend_Search_Lucene_Index_Term {
		$instance = Datamodel::getInstance($this->getTableName(), true);

		$raw_term = $term->text;
		if (mb_substr($raw_term, -1) == '|') {
			$raw_term = mb_substr($raw_term, 0, mb_strlen($raw_term) - 1);
		}

		$field_components = explode('/', $term->field);

		if ((strtolower($raw_term) === '[blank]')) {
			if ($instance instanceof BaseLabel) { // labels usually have actual [BLANK] values
				return new Zend_Search_Lucene_Index_Term(
					'"' . $raw_term . '"', $term->field
				);
			} else {
				return new Zend_Search_Lucene_Index_Term(
					$term->field, '_missing_'
				);
			}
		} elseif (strtolower($raw_term) === '[set]') {
			return new Zend_Search_Lucene_Index_Term(
				$term->field, '_exists_'
			);
		} elseif (
			($instance instanceof BaseModel) && isset($field_components[1])
			&& ($instance->getProperty('ID_NUMBERING_ID_FIELD') == $field_components[1])
		) {
			if (stripos($raw_term, '*') !== false) {
				return new Zend_Search_Lucene_Index_Term(
					$raw_term, $term->field
				);
			} else {
				return new Zend_Search_Lucene_Index_Term(
					'"' . $raw_term . '"', $term->field
				);
			}
		} else {
			return new Zend_Search_Lucene_Index_Term(
				str_replace('/', '\\/', $raw_term),
				$term->field
			);
		}
	}

	/**
	 * @return string
	 */
	public function getKey(): string {
		return $this->getTableName() . '/' . $this->getFieldName();
	}

}
