<?php
/** ---------------------------------------------------------------------
 * app/lib/Plugins/SearchEngine/Elastic8.php :
 * ----------------------------------------------------------------------
 * CollectiveAccess
 * Open-source collections management software
 * ----------------------------------------------------------------------
 *
 * Software by Whirl-i-Gig (http://www.whirl-i-gig.com)
 * Copyright 2015-2021 Whirl-i-Gig
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

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\Exception\AuthenticationException;
use Elastic\ElasticSearch\Exception\ClientResponseException;
use Elastic\Elasticsearch\Exception\ElasticsearchException;
use Elastic\Elasticsearch\Exception\MissingParameterException;
use Elastic\Elasticsearch\Exception\ServerResponseException;
use Elastic8\FieldException;
use Monolog\Handler\ErrorLogHandler;
use Elastic8\Mapping;

require_once(__CA_LIB_DIR__ . '/Configuration.php');
require_once(__CA_LIB_DIR__ . '/Datamodel.php');
require_once(__CA_LIB_DIR__ . '/Plugins/WLPlug.php');
require_once(__CA_LIB_DIR__ . '/Plugins/IWLPlugSearchEngine.php');
require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/BaseSearchPlugin.php');
require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8Result.php');

require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/Field.php');
require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/Mapping.php');
require_once(__CA_LIB_DIR__ . '/Plugins/SearchEngine/Elastic8/Query.php');

class WLPlugSearchEngineElastic8 extends BaseSearchPlugin implements IWLPlugSearchEngine {
	protected const LOG_LEVEL_DEFAULT = 'WARNING';
	/** @var int size is Java's 32bit int, for ElasticSearch */
	protected const ES_MAX_INT = 2147483647;
	protected const SCROLL_TIMEOUT = '1m';
	protected array $index_content_buffer = [];

	protected ?string $indexing_subject_tablenum = null;
	protected ?int $indexing_subject_row_id = null;
	protected ?string $indexing_subject_tablename = null;

	static protected ?Client $client = null;

	static private array $doc_content_buffer = [];
	static private array $update_content_buffer = [];
	static private array $delete_buffer = [];
	static private array $record_cache = [];

	protected string $elasticsearch_index_name = '';
	protected string $elasticsearch_base_url = '';

	/**
	 * @throws AuthenticationException
	 */
	public function __construct($db = null) {
		parent::__construct($db);

		// allow overriding settings from search.conf via constant (usually defined in bootstrap file)
		// this is useful for multi-instance setups which have the same set of config files for multiple instances
		if (defined('__CA_ELASTICSEARCH_BASE_URL__') && (strlen(__CA_ELASTICSEARCH_BASE_URL__) > 0)) {
			$this->elasticsearch_base_url = __CA_ELASTICSEARCH_BASE_URL__;
		} else {
			$this->elasticsearch_base_url = $this->search_config->get('search_elasticsearch_base_url');
		}
		$this->elasticsearch_base_url = trim($this->elasticsearch_base_url,
			"/");   // strip trailing slashes as they cause errors with ElasticSearch 5.x

		if (defined('__CA_ELASTICSEARCH_INDEX_NAME__') && (strlen(__CA_ELASTICSEARCH_INDEX_NAME__) > 0)) {
			$this->elasticsearch_index_name = __CA_ELASTICSEARCH_INDEX_NAME__;
		} else {
			$this->elasticsearch_index_name = $this->search_config->get('search_elasticsearch_index_name');
		}

		$this->getClient();
	}

	/**
	 * Get ElasticSearch index name prefix
	 */
	protected function getIndexNamePrefix(): string {
		return $this->elasticsearch_index_name;
	}

	/**
	 * Get ElasticSearch index name
	 */
	protected function getIndexName($table): string {
		if (is_numeric($table)) {
			$table = Datamodel::getTableName($table);
		}

		return $this->getIndexNamePrefix() . "_{$table}";
	}

	/**
	 * Refresh ElasticSearch mapping if necessary
	 *
	 * @param bool $force force refresh if set to true [default is false]
	 *
	 * @throws Exception
	 */
	public function refreshMapping(bool $force = false) {

		/** @var Mapping $mapping */
		static $mapping;
		if (!$mapping) {
			$mapping = new Mapping();
		}

		if ($force) {
			$indexPrefix = $this->getIndexNamePrefix();
			// TODO: Move away from plain index template in favour of composable templates when the ES PHP API supports them.
			$indexSettings = [
				'settings' => [
					'max_result_window' => self::ES_MAX_INT,
					'analysis' => [
						'analyzer' => [
							'keyword_lowercase' => [
								'tokenizer' => 'keyword',
								'filter' => 'lowercase'
							],
							'whitespace' => [
								'tokenizer' => 'whitespace',
								'filter' => 'lowercase'
							],
						],
						'normalizer' => [
							'lowercase_normalizer' => [
								'type' => 'custom',
								'filter' => ['lowercase']
							]
						]
					],
					'index.mapping.total_fields.limit' => 20000,
				],
				'mappings' => [
					'_source' => [
						'enabled' => true,
					],
					'dynamic' => true,
					'dynamic_templates' => $mapping->getDynamicTemplates(),
				]
			];
			$client = $this->getClient();
			$indices = $client->indices();
			$indices->putTemplate([
				'name' => $indexPrefix,
				'body' => ['index_patterns' => [$indexPrefix . "_*"]] + $indexSettings
			]);
			foreach ($mapping->getTables() as $table) {
				$indexName = $this->getIndexName($table);
				if (!$indices->exists(['index' => $indexName, 'ignore_missing' => true])->asBool()) {
					$indices->create(['index' => $indexName]);
				}
				$indices->putSettings([
					'index' => $indexName,
					'reopen' => true,
					'body' => $indexSettings['settings']
				]);
				$indices->putMapping(['index' => $indexName, 'body' => $indexSettings['mappings']]);
			}
		}
	}

	/**
	 * Get ElasticSearch client
	 *
	 * @return Client
	 * @throws AuthenticationException
	 */
	protected function getClient(): Client {
		if (!self::$client) {
			$logger = new \Monolog\Logger('elasticsearch');
			$log_level = self::LOG_LEVEL_DEFAULT;
			if (defined('__CA_ELASTICSEARCH_LOG_LEVEL__')) {
				$log_level = __CA_ELASTICSEARCH_LOG_LEVEL__;
			}
			$monolog_level = \Monolog\Logger::toMonologLevel($log_level);
			$logger->pushHandler(new ErrorLogHandler(null, $monolog_level));
			self::$client = Elastic\Elasticsearch\ClientBuilder::create()
				->setHosts([$this->elasticsearch_base_url])
				->setLogger($logger)
				->setRetries(3)
				->build();
		}

		return self::$client;
	}

	public function init() {
		if (($max_indexing_buffer_size = (int) $this->search_config->get('elasticsearch_indexing_buffer_size'))
			< 1
		) {
			$max_indexing_buffer_size = 250;
		}

		$this->options = [
			'start' => 0,
			'limit' => self::ES_MAX_INT,
			// maximum number of hits to return [default=100000],
			'maxIndexingBufferSize' => $max_indexing_buffer_size,
			// maximum number of indexed content items to accumulate before writing to the index,
		];

		$this->capabilities = [
			'incremental_reindexing' => false,
			'sort' => true
		];
	}

	/**
	 * Completely clear index for a CA table (usually in preparation for a full reindex)
	 *
	 * @param int|null $table_num
	 *
	 * @return bool
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws MissingParameterException
	 * @throws ServerResponseException
	 * @throws Exception
	 */
	public function truncateIndex(?int $table_num = null): bool {
		$mapping = new Elastic8\Mapping();
		if ($table_num) {
			$tables = [Datamodel::getTableName($table_num)];
		} else {
			$tables = $mapping->getTables();
		}
		$this->getClient()->indices()->delete([
			'index' => array_map([$this, 'getIndexName'], $tables),
			'ignore_unavailable' => true
		]);
		$this->refreshMapping(true);

		return true;
	}

	public function setTableNum($table_num) {
		$this->indexing_subject_tablenum = $table_num;
	}


	/**
	 * @throws ApplicationException
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws ServerResponseException
	 */
	public function __destruct() {
		if (!defined('__CollectiveAccess_Installer__') || !__CollectiveAccess_Installer__) {
			$this->flushContentBuffer();
		}
	}

	/**
	 * Do search
	 *
	 * @param int $subject_tablenum
	 * @param string $search_expression
	 * @param array $filters
	 * @param null|Zend_Search_Lucene_Search_Query_Boolean $rewritten_query
	 *
	 * @return WLPlugSearchEngineElastic8Result
	 * @throws AuthenticationException
	 * @throws ServerResponseException
	 * @throws Zend_Search_Lucene_Exception
	 * @throws ApplicationException
	 */
	public function search(
		int $subject_tablenum, string $search_expression, array $filters = [], $rewritten_query = null
	): WLPlugSearchEngineElastic8Result {
		Debug::msg("[ElasticSearch] incoming search query is: {$search_expression}");
		Debug::msg("[ElasticSearch] incoming query filters are: " . print_r($filters, true));

		try {
			$query = new Elastic8\Query($subject_tablenum, $search_expression, $rewritten_query, $filters);
			$query_string = $query->getSearchExpression();
		}
		catch (Exception $e) {
			$this->postError(1710, _t('Error building search expression. ' . $e->getMessage()), _t('Building ElasticSearch search expression'));
			return new WLPlugSearchEngineElastic8Result([], $subject_tablenum);
		}


		Debug::msg("[ElasticSearch] actual search query sent to ES: {$query_string}");

		$limit = self::ES_MAX_INT;
		$sort_direction = 'asc';
		$tableName = Datamodel::getTableName($subject_tablenum);
		$request = AppController::getInstance()->getRequest();
		$context = ResultContext::getResultContextForLastFind($request, $subject_tablenum);
		$start = 0;
		$isExport = (bool)$request->getParameter('export_format', pString)
			|| $request->getParameter('mode', pString) === 'from_results';
		$page = null;
		if (!$isExport) {
			$start = $request->getParameter('start', pInteger) ?: $this->getOption('start');
			$context_page = $context->searchExpressionHasChanged() ? null : $context->getCurrentResultsPageNumber();
			$page = $request->getParameter('page', pInteger) ?: $context_page;
			$page = $page ?: 1;
			$limit = $context->getItemsPerPage();
			$limit = $request->getParameter('limit', pInteger) ?: $limit;
			// In case we don't have a limit, set a friendly one here.
			$limit = $limit ?: $request->config->get('items_per_page_default_for_' . $tableName . '_search');
			// TODO Store and retrieve the scroll_id instead of just returning all results.
		}
		$sort = $request->getParameter('sort', pString) ?: $context->getCurrentSort();
		$sort = $sort ?: $this->getOption('sort');
		// Sort by relevance if we don't have any sort set
		$sort = $sort ?: '_natural';
		$sort_direction = $request->getParameter('direction', pString) === 'desc' ? 'desc' : $sort_direction;
		$sort_direction = $sort_direction ?: $context->getCurrentSortDirection();
		if ($page) {
			$start = $limit * ($page - 1);
		}
		Debug::msg("[ElasticSearch] Start: $start Page: $page Limit: $limit");
		// If we're going to sort by preferred labels, just let the 'name' case handle it.
		if (preg_match("/$tableName\.preferred_labels/", $sort)) {
			$sort = 'name';
		}
		// relevance, idno and name come from QuickSearch select box.
		switch ($sort) {
			case 'relevance':
			case '_natural':
				$sort = '_score';
				break;
			case 'idno':
				$sort = sprintf('%s.%s',
					$tableName,
					Datamodel::getTableProperty($subject_tablenum, 'ID_NUMBERING_SORT_FIELD')
				);
				$sort = $query->getSortKey($sort);
				break;
			case 'name':
				$vs_label_table = Datamodel::getTableProperty($subject_tablenum, 'LABEL_TABLE_NAME');
				$sort = sprintf('%s.%s',
					Datamodel::getTableName($vs_label_table),
					Datamodel::getTableProperty($vs_label_table, 'LABEL_SORT_FIELD')
				);
				$sort = $query->getSortKey($sort);
				break;
			default:
				$sort = $query->getSortKey($sort);
				break;
		}
		$sort = preg_replace('/^(\w+)\./', '\1/', $sort);
		$search_params = [
			'index' => $this->getIndexName($subject_tablenum),
//			'scroll' => self::SCROLL_TIMEOUT,
			'body' => [
				'sort' => $sort ? [$sort => ['order' => $sort_direction]] : null,
				'from' => $start,
				'size' => $limit,
				'track_total_hits' => true,
				'_source' => false,
				'query' => [
					'bool' => [
						'must' => [
							[
								'query_string' => [
									'analyze_wildcard' => true,
									'query' => $query_string,
									'default_operator' => 'AND',
									'default_field' => '_all',
								],
							]
						]
					]
				]
			]
		];

		// apply additional filters that may have been set by the query
		if (($additional_filters = $query->getAdditionalFilters()) && is_array($additional_filters)
			&& (sizeof($additional_filters) > 0)
		) {
			foreach ($additional_filters as $filter) {
				$search_params['body']['query']['bool']['must'][] = $filter;
			}
		}

		Debug::msg("[ElasticSearch] actual query filters are: " . print_r($additional_filters, true));
		try {
			$results = $this->getClient()->search($search_params);
		} catch (ClientResponseException $e) {
			$this->getLogger()->logError($e->getMessage());
			if (preg_match('!No mapping found for \[(\w+/[\w.\,-|]+)] in order to sort on!', $e->getMessage(), $matches)){
				$search_params['body']['sort'] = [];
				//retry search without sort parameters
				$this->postError(1710, _t('Cannot sort results by [%1]. Reverting to sorting by relevance.', $matches[1]), _t('Search sorting.'));
				$this->getLogger()->logInfo(_t('Retried search with default sort'));
				$results = $this->getClient()->search($search_params);
			} else {
				$this->postError(1710, _t('Cannot perform search correctly and no results returned. Please consult the application error log for more information.', $e->getMessage()), _t('Querying ElasticSearch'));
				$results = ['hits' => ['hits' => [], ['total' => ['value' => 0]]]];
			}
		}

		$hits = $results['hits']['hits'];
		$hits = array_combine(array_column($hits, '_id'), $hits);
		$result = new WLPlugSearchEngineElastic8Result($hits, $subject_tablenum);
		Paginator::getInstance($result)->setNumHits($results['hits']['total']['value'] ?? 0);
		$context->setParameter('scroll_id', $results['_scroll_id']);
		return $result;
	}

	/**
	 * Start row indexing
	 *
	 * @param int $subject_tablenum
	 * @param int $subject_row_id
	 */
	public function startRowIndexing(int $subject_tablenum, int $subject_row_id): void {
		$this->index_content_buffer = [];
		$this->indexing_subject_tablenum = $subject_tablenum;
		$this->indexing_subject_row_id = $subject_row_id;
		$this->indexing_subject_tablename = Datamodel::getTableName($subject_tablenum);
	}

	/**
	 * Index field
	 *
	 * @param mixed $content
	 *
	 * @throws Exception
	 */
	public function indexField(
		int $content_tablenum, string $content_fieldname, int $content_row_id, $content,
		?array $options = null
	): void {
		$field = new Elastic8\Field($content_tablenum, $content_fieldname);
		if (!is_array($content)) {
			$content = [$content];
		}

		foreach ($content as $ps_content) {
			$fragment = $field->getIndexingFragment($ps_content, $options);

			foreach ($fragment as $key => $val) {
				$this->index_content_buffer[$key][] = $val;
			}
		}
	}

	/**
	 * Commit indexing for row
	 * That doesn't necessarily mean it's actually written to the index.
	 * We still keep the data local until the document buffer is full.
	 *
	 * @throws ApplicationException
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws ServerResponseException
	 */
	public function commitRowIndexing() {
		if (sizeof($this->index_content_buffer) > 0) {
			self::$doc_content_buffer[$this->indexing_subject_tablename . '/' .
			$this->indexing_subject_row_id]
				= $this->index_content_buffer;
		}

		unset($this->indexing_subject_tablenum);
		unset($this->indexing_subject_row_id);
		unset($this->indexing_subject_tablename);

		$this->flushBufferWhenFull();
	}

	/**
	 * Delete indexing for row
	 *
	 * @param array|null $field_nums
	 *
	 * @throws ApplicationException
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws MissingParameterException
	 * @throws ServerResponseException
	 * @throws Exception
	 */
	public function removeRowIndexing(
		int $subject_tablenum, int $subject_row_id, ?int $field_tablenum = null, $field_nums = null,
		?int $field_row_id = null, ?int $rel_type_id = null
	) {
		$table = Datamodel::getTableName($subject_tablenum);
		// if the field table num is set, we only remove content for this field and don't nuke the entire record!
		if ($field_tablenum) {
			if (is_array($field_nums)) {
				foreach ($field_nums as $content_fieldnum) {
					$field = new Elastic8\Field($field_tablenum, $content_fieldnum);
					$fragment = $field->getIndexingFragment('');

					// fetch the record
					try {
						$record = $this->getSourceRecord($table, $subject_row_id);
					} catch (ClientResponseException $e) {
						// record is gone?
						unset(self::$update_content_buffer[$table][$subject_row_id]);
						continue;
					}

					foreach ($fragment as $key => $val) {
						if (isset($record[$key])) {
							$values = $record[$key];

							// we reindex both value and index arrays here, starting at 0
							// json_encode seems to treat something like array(1=>'foo') as object/hash, rather than a list .. which is not good
							self::$update_content_buffer[$table][$subject_row_id][$key]
								= array_values($values);
						}
					}
				}
			}

			$this->flushBufferWhenFull();

		} else {
			// queue record for removal -- also make sure we don't try do any unnecessary indexing
			unset(self::$update_content_buffer[$table][$subject_row_id]);
			self::$delete_buffer[$table][] = $subject_row_id;
		}
	}

	/**
	 * Flush content buffer and write to index
	 *
	 * @throws ClientResponseException
	 * @throws AuthenticationException
	 * @throws ServerResponseException
	 * @throws ApplicationException
	 */
	public function flushContentBuffer() {

		$bulk_params = [];

		// @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/updating_documents.html#_upserts


		// delete docs
		foreach (self::$delete_buffer as $table => $rows) {
			foreach (array_unique($rows) as $row_id) {
				$bulk_params['body'][] = [
					'delete' => [
						'_index' => $this->getIndexName($table),
						'_id' => $row_id
					]
				];

				// also make sure we don't do unnecessary indexing for this record below
				unset(self::$update_content_buffer[$table][$row_id]);
			}
		}

		// newly indexed docs
		foreach (self::$doc_content_buffer as $key => $doc_content_buffer) {
			$tmp = explode('/', $key);
			$table = $tmp[0];
			$primary_key = intval($tmp[1]);

			$f = [
				'update' => [
					'_index' => $this->getIndexName($table),
					'_id' => $primary_key
				]
			];
			$bulk_params['body'][] = $f;

			// add changelog to index
			$doc_content_buffer = array_merge(
				$doc_content_buffer,
				$this->getChangeLogFragment($table, $primary_key)
			);

			$bulk_params['body'][] = ['doc' => $doc_content_buffer, 'doc_as_upsert' => true];
		}

		// update existing docs
		foreach (self::$update_content_buffer as $table => $rows) {
			foreach ($rows as $row_id => $fragment) {

				$f = [
					'update' => [
						'_index' => $this->getIndexName($table),
						'_id' => (int) $row_id
					]
				];
				$bulk_params['body'][] = $f;

				// add changelog to fragment
				$fragment = array_merge(
					$fragment,
					$this->getChangeLogFragment($table, $row_id)
				);

				$bulk_params['body'][] = ['doc' => $fragment, 'doc_as_upsert' => true];
			}
		}

		if (!empty($bulk_params['body'])) {
			// Improperly encoded UTF8 characters in the body will make
			// Elastic throw errors and result in records being omitted from the index.
			// We force the document to UTF8 here to avoid that fate.
			$bulk_params['body'] = caEncodeUTF8Deep($bulk_params['body']);

			try {
				$responses = $this->getClient()->bulk($bulk_params);
				if ($responses['errors']) {
					// Log errors for each operation
					foreach ($responses['items'] as $item) {
						if (isset($item['index']['error'])) {
							// Log index error
							$errors[] = "Indexing error: " . json_encode($item['index']['error']);
						} elseif (isset($item['update']['error'])) {
							// Log update error
							$errors[] = "Update error: " . json_encode($item['update']['error']);
						}
					}

					// If there are errors, throw ApplicationException
					if (!empty($errors)) {
						$message = _t("%1 out of %2 bulk operation(s) failed. Errors: %3.", count($errors),
							count($responses['items']), implode('; ', $errors));
						$this->getClient()->getLogger()->error($message);
						error_log($message);
						throw new ApplicationException($message);
					}
				}
			} catch (ElasticsearchException $e) {
				throw new ApplicationException(_t('Indexing error %1', $e->getMessage()));
			}

			// we usually don't need indexing to be available *immediately* unless we're running automated tests of course :-)
			if (caIsRunFromCLI() && $this->getIndexNamePrefix()
				&& (!defined('__CollectiveAccess_IS_REINDEXING__')
					|| !__CollectiveAccess_IS_REINDEXING__)
			) {
				$mapping = new Elastic8\Mapping();

				foreach ($mapping->getTables() as $table) {
					$this->getClient()->indices()->refresh(['index' => $this->getIndexName($table)]);
				}
			}
		}

		$this->index_content_buffer = [];
		self::$doc_content_buffer = [];
		self::$update_content_buffer = [];
		self::$delete_buffer = [];
		self::$record_cache = [];
	}

	public function getChangeLogFragment($table_name, $row_id): array {
		$content = caGetChangeLogForElasticSearch(
			$this->db,
			Datamodel::getTableNum($table_name),
			$row_id
		);

		$field = new Elastic8\FieldTypes\ChangeLogDate('changeLog');

		return $field->getIndexingFragment($content, []);
	}

	/**
	 * Set additional index-level settings like analyzers or token filters
	 *
	 * @param int $tablenum
	 *
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws ServerResponseException
	 */

	public function optimizeIndex(int $tablenum) {
		$this->getClient()->indices()->forceMerge(['index' => $this->getIndexName($tablenum)]);
	}

	public function engineName(): string {
		return 'Elastic8';
	}

	/**
	 * Performs the quickest possible search on the index for the specified table_num in $table_num
	 * using the text in $ps_search. Unlike the search() method, quickSearch doesn't support
	 * any sort of search syntax. You give it some text and you get a collection of (hopefully) relevant results back
	 * quickly. quickSearch() is intended for autocompleting search suggestion UIs and the like, where performance is
	 * critical and the ability to control search parameters is not required.
	 *
	 * @param $pn_table_num - The table index to search on
	 * @param $ps_search - The text to search on
	 * @param array $pa_options - an optional associative array specifying search options. Supported options are:
	 *     'limit'
	 *     (the maximum number of results to return)
	 *
	 * @return array - an array of results is returned keyed by primary key id. The array values boolean true. This is
	 *     done to ensure no duplicate row_ids
	 * @throws AuthenticationException
	 * @throws ServerResponseException
	 * @throws Zend_Search_Lucene_Exception
	 */
	public function quickSearch($pn_table_num, $ps_search, $pa_options = []): array {
		if (!is_array($pa_options)) {
			$pa_options = [];
		}
		$limit = caGetOption('limit', $pa_options, 0);

		$result = $this->search($pn_table_num, $ps_search);
		$pks = $result->getPrimaryKeyValues();
		if ($limit) {
			$pks = array_slice($pks, 0, $limit);
		}

		return array_flip($pks);
	}

	public function isReindexing(): bool {
		return (defined('__CollectiveAccess_IS_REINDEXING__') && __CollectiveAccess_IS_REINDEXING__);
	}

	/**
	 * @return void
	 * @throws ApplicationException
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws ServerResponseException
	 */
	private function flushBufferWhenFull(): void {
		if ((
				sizeof(self::$doc_content_buffer) +
				sizeof(self::$update_content_buffer) +
				sizeof(self::$delete_buffer)
			) > $this->getOption('maxIndexingBufferSize')
		) {
			$this->flushContentBuffer();
		}
	}

	/**
	 * @param $table
	 * @param int $subject_row_id
	 *
	 * @return mixed
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws MissingParameterException
	 * @throws ServerResponseException
	 */
	public function getSourceRecord($table, int $subject_row_id) {
		return $this->getClient()->get([
			'index' => $this->getIndexName($table),
			'id' => $subject_row_id
		])['_source'];
	}

	/**
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws ServerResponseException
	 */
	public function info() {
		return $this->getClient()->info();
	}

	/**
	 * @throws AuthenticationException
	 * @throws ClientResponseException
	 * @throws ServerResponseException
	 * @throws MissingParameterException
	 */
	public function checkIndexes(): bool {
		$mapping = new Elastic8\Mapping();
		$tables = $mapping->getTables();

		return $this->getClient()->indices()->exists([
			'index' => array_map([$this, 'getIndexName'], $tables),
			'ignore_missing' => true,
		])->asBool();
	}

	/**
	 * @throws ApplicationException
	 */
	private function getLogger() {
		static $logger;
		if (!$logger) {
			$logger = caGetLogger();
		}

		return $logger;
	}
}
