<?php
/** ---------------------------------------------------------------------
 * app/lib/Plugins/SearchEngine/Elastic8ConfigurationSettings.php :
 * ----------------------------------------------------------------------
 * CollectiveAccess
 * Open-source collections management software
 * ----------------------------------------------------------------------
 *
 * Software by Whirl-i-Gig (http://www.whirl-i-gig.com)
 * Copyright 2012-2015 Whirl-i-Gig
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
 * @package    CollectiveAccess
 * @subpackage Search
 * @license    http://www.gnu.org/copyleft/gpl.html GNU Public License version 3
 *
 * ----------------------------------------------------------------------
 */

/* is ElasticSearch running?  */

define('__CA_ELASTICSEARCH_SETTING_RUNNING__', 5001);
/* does the index exist? */
define('__CA_ELASTICSEARCH_SETTING_INDEXES_EXIST__', 5002);
require_once(__CA_LIB_DIR__ . '/Datamodel.php');
require_once(__CA_LIB_DIR__ . '/Configuration.php');
require_once(__CA_LIB_DIR__ . '/Search/SearchBase.php');
require_once(__CA_LIB_DIR__ . '/Search/ASearchConfigurationSettings.php');
require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8.php');

class Elastic8ConfigurationSettings extends ASearchConfigurationSettings {
	private array $setting_names = [];
	private array $setting_descriptions = [];
	private array $setting_hints = [];

	private WLPlugSearchEngineElastic8 $elastic8;

	public function __construct() {
		$this->_initMessages();

		$this->elastic8 = new WLPlugSearchEngineElastic8();

		parent::__construct();
	}

	public function getEngineName(): string {
		return "Elastic8";
	}

	private function _initMessages() {
		$this->setting_names[__CA_ELASTICSEARCH_SETTING_RUNNING__]
			= _t("ElasticSearch up and running");
		$this->setting_names[__CA_ELASTICSEARCH_SETTING_INDEXES_EXIST__]
			= _t("ElasticSearch indexes exist");
		$this->setting_descriptions[__CA_ELASTICSEARCH_SETTING_RUNNING__]
			= _t("The ElasticSearch service must be running.");
		$this->setting_descriptions[__CA_ELASTICSEARCH_SETTING_INDEXES_EXIST__]
			= _t("CollectiveAccess uses multiple indexes in an ElasticSearch setup.");
		$this->setting_hints[__CA_ELASTICSEARCH_SETTING_RUNNING__]
			= _t("Install and start the ElasticSearch service. If it is already running, check your CollectiveAccess configuration (the ElasticSearch URL in particular).");
		$this->setting_hints[__CA_ELASTICSEARCH_SETTING_INDEXES_EXIST__]
			= _t("If the service is running and can be accessed by CollectiveAccess but the indexes are missing, let CollectiveAccess generate fresh indexes and create the related indexing mappings. There is a tool in support/bin/caUtils.");
	}

	public function setSettings() {
		$this->opa_possible_errors = array_keys($this->setting_names);
	}

	public function checkSetting($pn_setting_num) {
		switch ($pn_setting_num) {
			case __CA_ELASTICSEARCH_SETTING_RUNNING__:
				return $this->_checkElasticSearchRunning();
			case __CA_ELASTICSEARCH_SETTING_INDEXES_EXIST__:
				return $this->_checkElasticSearchIndexesExist();
			default:
				return false;
		}
	}

	public function getSettingName($pn_setting_num) {
		return $this->setting_names[$pn_setting_num];
	}

	public function getSettingDescription($pn_setting_num) {
		return $this->setting_descriptions[$pn_setting_num];
	}

	public function getSettingHint($pn_setting_num) {
		return $this->setting_hints[$pn_setting_num];
	}

	private function _checkElasticSearchRunning(): int {
		try {
			$this->elastic8->info();
			return __CA_SEARCH_CONFIG_OK__;
		} catch (\Exception $e) {
			return __CA_SEARCH_CONFIG_ERROR__;
		}
	}

	private function _checkElasticSearchIndexesExist(): int {
		try {
			if ($this->elastic8->checkIndexes()) {
				return __CA_SEARCH_CONFIG_OK__;
			}
		} catch (\Exception $e) {
			return __CA_SEARCH_CONFIG_ERROR__;
		}

		return __CA_SEARCH_CONFIG_ERROR__;
	}
}
