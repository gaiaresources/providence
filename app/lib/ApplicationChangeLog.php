<?php
/** ---------------------------------------------------------------------
 * app/lib/ApplicationChangeLog.php : class for interacting with the application database change log
 * ----------------------------------------------------------------------
 * CollectiveAccess
 * Open-source collections management software
 * ----------------------------------------------------------------------
 *
 * Software by Whirl-i-Gig (http://www.whirl-i-gig.com)
 * Copyright 2009-2019 Whirl-i-Gig
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
 * @subpackage BaseModel
 * @license http://www.gnu.org/copyleft/gpl.html GNU Public License version 3
 *
 * ----------------------------------------------------------------------
 */
 
  /**
  *
  */
  
require_once(__CA_LIB_DIR__."/Configuration.php");
require_once(__CA_LIB_DIR__."/Datamodel.php");
require_once(__CA_LIB_DIR__."/Db.php");
 
 class ApplicationChangeLog {
 	# ----------------------------------------------------------------------
 	private $ops_change_log_database = '';
	private $opb_dont_show_timestamp_in_change_log = false;
 	# ----------------------------------------------------------------------
 	public function __construct() {
 		$o_config = Configuration::load();
		if ($this->ops_change_log_database = $o_config->get("change_log_database")) {
			$this->ops_change_log_database .= ".";
		}

		$this->opb_dont_show_timestamp_in_change_log = (bool) $o_config->get('dont_show_timestamp_in_change_log');
 	}
 	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	public function getRecentChangesForDisplay($pn_table_num, $pn_num_seconds=604800, $pn_limit=0, $po_request=null, $ps_css_id=null) {	// 604800 = number of seconds in one week
 		return $this->_getLogDisplayOutput($this->_getChangeLogFromRawData($this->getRecentChanges($pn_table_num, $pn_num_seconds, $pn_limit), $pn_table_num, array('return_item_names' => true)), array('id' => $ps_css_id, 'request' => $po_request));
 	}
 	# ----------------------------------------
 	/**
 	 *
 	 */
	public function getChangeLogForRowForDisplay($t_item, $ps_css_id=null, $pn_user_id=null) {
		return $this->_getLogDisplayOutputForRow($this->getChangeLogForRow($t_item, array('user_id' => $pn_user_id)), array('id' => $ps_css_id));
	}
	# ----------------------------------------
	/**
 	 *
 	 */
	public function getChangeLogForRow($t_item, $pa_options=null) {
		return $this->_getChangeLogFromRawData($t_item->getChangeLog($t_item->getPrimaryKey(), $pa_options), $t_item->tableNum(), $pa_options);
	}
	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	 public function getRecentChanges($pn_table_num, $pn_num_seconds=604800, $pn_limit=0) {	
		return $this->_getChangeLogFromRawData($this->getRecentChangesAsRawData($pn_table_num, $pn_num_seconds, $pn_limit), $pn_table_num,  array('return_item_names' => true));
	}
 	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	public function getRecentChangesAsRawData($pn_table_num, $pn_num_seconds=604800, $pn_limit=0) {	// 604800 = number of seconds in one week
		$o_db = new Db();
		$qs_log = $o_db->prepare("
			SELECT DISTINCT
				wcl.log_id, wcl.log_datetime log_datetime, wcl.user_id, wcl.changetype, wcl.logged_table_num, wcl.logged_row_id,
				 wcl.unit_id, wu.email, wu.fname, wu.lname, wcls.subject_table_num, wcls.subject_row_id /* wclsnap.snapshot, */
			FROM ".$this->ops_change_log_database.".ca_change_log wcl
			INNER JOIN ".$this->ops_change_log_database.".ca_change_log_snapshots AS wclsnap ON wclsnap.log_id = wcl.log_id
			LEFT JOIN ".$this->ops_change_log_database.".ca_change_log_subjects AS wcls ON wcl.log_id = wcls.log_id
			LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
			WHERE
				(
					((wcl.logged_table_num = ?) AND (wcls.subject_table_num IS NULL))
					OR
					(wcls.subject_table_num = ?)
				)
				AND (wcl.log_datetime > ?)
			ORDER BY wcl.log_datetime DESC
		");
		
		if ($pn_limit > 0) {
			$qs_log->setLimit($pn_limit);
		}
		
		if ($qr_res = $qs_log->execute($pn_table_num, $pn_table_num, (time() - $pn_num_seconds))) {
			$va_log = array();
			while($qr_res->nextRow()) {
				$va_log[] = $qr_res->getRow();
				$va_log[sizeof($va_log)-1]['snapshot'] = caUnserializeForDatabase($va_log[sizeof($va_log)-1]['snapshot']);
			}
			return array_reverse($va_log);
		}
		
		return array();
 	}
	# ----------------------------------------
 	/**
 	 *
 	 */
 	private function _getLogDisplayOutputForRow($pa_log, $pa_options=null) {
 		$ps_id = (isset($pa_options['id']) && $pa_options['id']) ? $pa_options['id'] : '';
 		$vs_output = '';
 		
 		if ($ps_id) {
 		$vs_output .= '<script language="JavaScript" type="text/javascript">
/* <![CDATA[ */
	$(document).ready(function(){
		$("#'.$ps_id.'").caFormatListTable();
	});
/* ]]> */
</script>';
		}
 		$vs_output .= '<table '.($ps_id ? 'id="'.$ps_id.'"' : '').' class="listtable">
		<thead>
			<tr>
				<th class="list-header-unsorted">
					'._t('Date').'
				</th>
				<th class="list-header-unsorted">
					 '._t('User').'
				</th>
				<th class="list-header-unsorted">
					'._t('Changes').'
				</th>
			</tr>
		</thead>
		<tbody>';
 		
	
		if (!sizeof($pa_log)) {
			$vs_output .= "<tr><td colspan='3'><div class='contentError' align='center'>"._t('No change log available')."</div></td></tr>\n";
		} else {
			foreach(array_reverse($pa_log) as $vn_unit_id => $va_log_entries) {
				if (is_array($va_log_entries) && sizeof($va_log_entries)) {
					$vs_output .= "\t<tr>";
					$vs_output .= "<td>".$va_log_entries[0]['datetime']."</td>";
					
					if (trim($va_log_entries[0]['user_fullname'])) {
						$vs_output .= "<td>";
						if (trim($va_log_entries[0]['user_email'])) {
							$vs_output .= " <a href='mailto:".$va_log_entries[0]['user_email']."'>".$va_log_entries[0]['user_fullname']."</a>";
						} else {
							$vs_output .= $va_log_entries[0]['user_fullname'];
						}
						
						$vs_output .= "</td>";
					} else {
						$vs_output .= "<td> </td>";
					}
					
					$vs_output .= "<td>";
					foreach($va_log_entries as $va_log_entry) {
						foreach($va_log_entry['changes'] as $va_change) {
							$vs_output .= '<span class="logChangeLabel">'.$va_log_entry['changetype_display'].' '.$va_change['label'].'</span>: '.$va_change['description'].(isset($va_change['rel_typename']) ? ' ('.$va_change['rel_typename'].')' : '')."<br/>\n";
						}
					}
					$vs_output .= "</div></td>";
					$vs_output .= "</tr>\n";
				}
			}
		}
		$vs_output .= "</table>\n";
		
		return $vs_output;
 	}
 	# ----------------------------------------
 	/**
 	 *
 	 */
 	private function _getLogDisplayOutput($pa_log, $pa_options=null) {
 		$ps_id = (isset($pa_options['id']) && $pa_options['id']) ? $pa_options['id'] : '';
 		$vs_output = '';
 		
 		if ($ps_id) {
 		$vs_output .= '<script language="JavaScript" type="text/javascript">
/* <![CDATA[ */
	$(document).ready(function(){
		$("#'.$ps_id.'").caFormatListTable();
	});
/* ]]> */
</script>';
		}
 		$vs_output .= '<table '.($ps_id ? 'id="'.$ps_id.'"' : '').' class="listtable">
		<thead>
			<tr>
				<th class="list-header-unsorted">
					'._t('Date').'
				</th>
				<th class="list-header-unsorted">
					 '._t('User').'
				</th>
				<th class="list-header-unsorted">
					 '._t('Subject').'
				</th>
				<th class="list-header-unsorted">
					'._t('Changes').'
				</th>
			</tr>
		</thead>
		<tbody>';
 		
	
		if (!sizeof($pa_log)) {
			$vs_output .= "<tr><td colspan='4'><div class='contentError' align='center'>"._t('No change log available')."</div></td></tr>\n";
		} else {
			foreach(array_reverse($pa_log) as $vn_unit_id => $va_log_entries) {
				if (is_array($va_log_entries) && sizeof($va_log_entries)) {
					$vs_output .= "\t<tr>";
					$vs_output .= "<td>".$va_log_entries[0]['datetime']."</td>";
					
					if (trim($va_log_entries[0]['user_fullname'])) {
						$vs_output .= "<td>";
						if (trim($va_log_entries[0]['user_email'])) {
							$vs_output .= " <a href='mailto:".$va_log_entries[0]['user_email']."'>".$va_log_entries[0]['user_fullname']."</a>";
						} else {
							$vs_output .= $va_log_entries[0]['user_fullname'];
						}
						
						$vs_output .= "</td>";
					} else {
						$vs_output .= "<td> </td>";
					}
					
					if (isset($pa_options['request']) && $pa_options['request']) {
						$vs_output .= "<td><a href='".caEditorUrl($pa_options['request'], $va_log_entries[0]['subject_table_num'] , $va_log_entries[0]['subject_id'])."'>".$va_log_entries[0]['subject']."</a></td>";
					} else {
						$vs_output .= "<td>".$va_log_entries[0]['subject']."</td>";
					}
					
					$vs_output .= "<td>";
					foreach($va_log_entries as $va_log_entry) {
						foreach($va_log_entry['changes'] as $va_change) {
							$vs_output .= '<span class="logChangeLabel">'.$va_log_entry['changetype_display'].' '.$va_change['label'].'</span>: '.$va_change['description']."<br/>\n";
						}
					}
					$vs_output .= "</div></td>";
					$vs_output .= "</tr>\n";
				}
			}
		}
		$vs_output .= "</table>\n";
		
		return $vs_output;
 	}
 	# ----------------------------------------
	/**
 	 *
 	 */
	private function _getChangeLogFromRawData($pa_data, $pn_table_num, $pa_options=null) {
		//print "<pre>".print_r($pa_data, true)."</pre>\n";	
		$va_log_output = array();
		$vs_blank_placeholder = '&lt;'._t('BLANK').'&gt;';
		$o_tep = new TimeExpressionParser();
		
		if (!$pa_options) { $pa_options = array(); }
		$t_user = ($pn_user_id = caGetOption('user_id', $pa_options, null)) ? new ca_users($pn_user_id) : null;
		
		if (sizeof($pa_data)) {
			//
			// Init
			//
			$va_change_types = array(
				'I' => _t('Added'),
				'U' => _t('Edited'),
				'D' => _t('Deleted')
			);
			
			$vs_label_table_name = $vs_label_display_name = '';
			$t_item = Datamodel::getInstanceByTableNum($pn_table_num, true);
			
			$vs_label_table_name = $vn_label_table_num = $vs_label_display_name = null;
			if (method_exists($t_item, 'getLabelTableName') && $t_item->getLabelTableInstance()) {
				$t_item_label = $t_item->getLabelTableInstance();
				$vs_label_table_name = $t_item->getLabelTableName();
				$vn_label_table_num = $t_item_label->tableNum();
				$vs_label_display_name = $t_item_label->getProperty('NAME_SINGULAR');
			}
			
			//
			// Group data by unit
			//
			$va_grouped_data = array();
			foreach($pa_data as $va_log_entry) {
				$va_grouped_data[$va_log_entry['unit_id']]['ca_table_num_'.$va_log_entry['logged_table_num']][] = $va_log_entry;
			}
			
			//
			// Process units
			//
			$va_attributes = array();
			$vn_pseudo_unit_counter = 1;
			foreach($va_grouped_data as $vn_unit_id => $va_log_entries_by_table) {
				foreach($va_log_entries_by_table as $vs_table_key => $va_log_entries) {
					foreach($va_log_entries as $va_log_entry) {
						$va_changes = array();
						
						if (!is_array($va_log_entry['snapshot'])) { $va_log_entry['snapshot'] = array(); }
						
						//
						// Get date/time stamp for display
						//
						$o_tep->setUnixTimestamps($va_log_entry['log_datetime'], $va_log_entry['log_datetime']);
						if($this->opb_dont_show_timestamp_in_change_log) {
							$vs_datetime = $o_tep->getText(array('timeOmit' => true));
						} else {
							$vs_datetime = $o_tep->getText();
						}
						
						//
						// Get user name
						//
						$vs_user = $va_log_entry['fname'].' '.$va_log_entry['lname'];
						$vs_email = $va_log_entry['email'];
						
						// The "logged" table/row is the row to which the change was actually applied
						// The "subject" table/row is the row to which the change is considered to have been made for workflow purposes.
						//
						// For example: if an entity is related to an object, strictly speaking the logging occurs on the ca_objects_x_entities
						// row (ca_objects_x_entities is the "logged" table), but the subject is ca_objects since it's only in the context of the
						// object (and probably the ca_entities row as well) that you can about the change.
						//		
						$t_obj = Datamodel::getInstanceByTableNum($va_log_entry['logged_table_num'], true);	// get instance for logged table
						if (!$t_obj) { continue; }
						
						$vs_subject_display_name = '???';
						$vn_subject_row_id = null;
						$vn_subject_table_num = null;
						if (isset($pa_options['return_item_names']) && $pa_options['return_item_names']) {
							if (!($vn_subject_table_num = $va_log_entry['subject_table_num'])) {
								$vn_subject_table_num = $va_log_entry['logged_table_num'];
								$vn_subject_row_id = $va_log_entry['logged_row_id'];
							} else {
								$vn_subject_row_id = $va_log_entry['subject_row_id'];
							}
							
							if ($t_subject = Datamodel::getInstanceByTableNum($vn_subject_table_num, true)) {
								if ($t_subject->load($vn_subject_row_id)) {
									if (method_exists($t_subject, 'getLabelForDisplay')) {
										$vs_subject_display_name = $t_subject->getLabelForDisplay(false);
									} else {
										if ($vs_idno_field = $t_subject->getProperty('ID_NUMBERING_ID_FIELD')) {
											$vs_subject_display_name = $t_subject->getProperty('NAME_SINGULAR').' ['.$t_subject->get($vs_idno_field).']';
										} else {
											$vs_subject_display_name = $t_subject->getProperty('NAME_SINGULAR').' ['.$vn_subject_row_id.']';
										}
									}
								}
							}
						}
						
						//
						// Get item changes
						//
						
						// ---------------------------------------------------------------
						// is this an intrinsic field?
						if (($pn_table_num == $va_log_entry['logged_table_num'])) {
							foreach($va_log_entry['snapshot'] as $vs_field => $vs_value) {
								$va_field_info = $t_obj->getFieldInfo($vs_field);
								if (isset($va_field_info['IDENTITY']) && $va_field_info['IDENTITY']) { continue; }
								if (isset($va_field_info['DISPLAY_TYPE']) && $va_field_info['DISPLAY_TYPE'] == DT_OMIT) { continue; }
								if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $vs_field)) { continue; }	// does user have access to this bundle?
								
								if ((isset($va_field_info['DISPLAY_FIELD'])) && (is_array($va_field_info['DISPLAY_FIELD'])) && ($va_disp_fields = $va_field_info['DISPLAY_FIELD'])) {
									//
									// Lookup value in related table
									//
									if (!$vs_value) { continue; }
									if (sizeof($va_disp_fields)) {
										$va_rel = Datamodel::getManyToOneRelations($t_obj->tableName(), $vs_field);
										$va_rel_values = array();
											
										if ($t_rel_obj = Datamodel::getInstance($va_rel['one_table'], true)) {
											$t_rel_obj->load($vs_value);
											
											foreach($va_disp_fields as $vs_display_field) {
												$va_tmp = explode('.', $vs_display_field);
												if (($vs_tmp = $t_rel_obj->get($va_tmp[1])) !== '') { $va_rel_values[] = $vs_tmp; }
											}
										}	
										$vs_proc_val = join(', ', $va_rel_values);
									}
								} else {
									// Is field a foreign key?
									$va_keys = Datamodel::getManyToOneRelations($t_obj->tableName(), $vs_field);
									if (sizeof($va_keys)) {
										// yep, it's a foreign key
										$va_rel_values = array();
										
										if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $va_keys['one_table'])) { continue; }	// does user have access to this bundle?
								
										if ($t_rel_obj = Datamodel::getInstance($va_keys['one_table'], true)) {
											if ($t_rel_obj->load($vs_value)) {
												if (method_exists($t_rel_obj, 'getLabelForDisplay')) {
													$vs_proc_val = $t_rel_obj->getLabelForDisplay(false);
												} else {
													$va_disp_fields = $t_rel_obj->getProperty('LIST_FIELDS');
													foreach($va_disp_fields as $vs_display_field) {
														if (($vs_tmp = $t_rel_obj->get($vs_display_field)) !== '') { $va_rel_values[] = $vs_tmp; }
													}
													$vs_proc_val = join(' ', $va_rel_values);
												}
												if (!$vs_proc_val) { $vs_proc_val = '???'; }
											} else {
												$vs_proc_val = _t("Not set");
											}
										} else {
											$vs_proc_val = _t('Non-existent');
										}
									} else {
							
										// Adjust display of value for different field types
										switch($va_field_info['FIELD_TYPE']) {
											case FT_BIT:
												$vs_proc_val = $vs_value ? 'Yes' : 'No';
												break;
											default:
												$vs_proc_val = $vs_value;
												break;
										}
										
										if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $vs_field)) { continue; }	// does user have access to this bundle?
										
										// Adjust display of value for lists
										if ($va_field_info['LIST']) {
											$t_list = new ca_lists();
											if ($t_list->load(array('list_code' => $va_field_info['LIST']))) {
												$vn_list_id = $t_list->getPrimaryKey();
												$t_list_item = new ca_list_items();
												if ($t_list_item->load(array('list_id' => $vn_list_id, 'item_value' => $vs_value))) {
													$vs_proc_val = $t_list_item->getLabelForDisplay();
												}
											}
										} else {
											if ($va_field_info['BOUNDS_CHOICE_LIST']) {
												// TODO
											}
										}
									}
								}
								
								$va_changes[] = array(
									'label' => $va_field_info['LABEL'],
									'description' => (strlen((string)$vs_proc_val) ? $vs_proc_val : $vs_blank_placeholder),
									'value' => $vs_value
								);
							}
						}
													
						// ---------------------------------------------------------------
						// is this a label row?
						if ($va_log_entry['logged_table_num'] == $vn_label_table_num) {
							
							foreach($va_log_entry['snapshot'] as $vs_field => $vs_value) {
								$va_changes[] = array(
									'label' => $t_item_label->getFieldInfo($vs_field, 'LABEL'),
									'description' => $vs_value
								);
							}
						}
						
						// ---------------------------------------------------------------
						// is this an attribute?
						if ($va_log_entry['logged_table_num'] == 3) {	// attribute_values
							if ($t_element = ca_attributes::getElementInstance($va_log_entry['snapshot']['element_id'])) {
								
								if ($t_element->get('parent_id') && ($t_container = ca_attributes::getElementInstance($t_element->get('hier_element_id')))) {
									$vs_element_code = $t_container->get('element_code');
								} else {
									$vs_element_code = $t_element->get('element_code');
								}
								
								if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $vs_element_code)) { continue; }	// does user have access to this bundle?
							
								if ($o_attr_val = Attribute::getValueInstance($t_element->get('datatype'))) {
									$o_attr_val->loadValueFromRow($va_log_entry['snapshot']);
									$vs_attr_val = $o_attr_val->getDisplayValue();
								} else {
									$vs_attr_val = '?';
								}
								
								// Convert list-based attributes to text
								if ($vn_list_id = $t_element->get('list_id')) {
									$t_list = new ca_lists();
									$vs_attr_val = $t_list->getItemFromListForDisplayByItemID($vn_list_id, $vs_attr_val, true);
								}
								
								if (!$vs_attr_val) { 
									$vs_attr_val = $vs_blank_placeholder;
								}
								$vs_label = $t_element->getLabelForDisplay();
								$va_attributes[$va_log_entry['snapshot']['attribute_id']]['values'][] = array(
									'label' => $vs_label,
									'value' => $vs_attr_val
								);
								$va_changes[] = array(
									'label' => $vs_label,
									'description' => $vs_attr_val
								);
							}
						}
						
						// ---------------------------------------------------------------
						// is this a related (many-many) row?
						$va_keys = Datamodel::getOneToManyRelations($t_item->tableName(), $t_obj->tableName());
						if (sizeof($va_keys) > 0) {
							if (method_exists($t_obj, 'getLeftTableNum')) {
								if ($t_obj->getLeftTableNum() == $t_item->tableNum()) {
									// other side of rel is on right
									$t_related_table = Datamodel::getInstanceByTableNum($t_obj->getRightTableNum(), true);
									$t_related_table->load($va_log_entry['snapshot'][$t_obj->getRightTableFieldName()]);
								} else {
									// other side of rel is on left
									$t_related_table = Datamodel::getInstanceByTableNum($t_obj->getLeftTableNum(), true);
									$t_related_table->load($va_log_entry['snapshot'][$t_obj->getLeftTableFieldName()]);
								}
								$t_rel = Datamodel::getInstanceByTableNum($t_obj->tableNum(), true);
								
								if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $t_related_table->tableName())) { continue; }	// does user have access to this bundle?
							
								$va_changes[] = array(
									'label' => caUcFirstUTF8Safe($t_related_table->getProperty('NAME_SINGULAR')),
									'idno' => ($vs_idno_field = $t_related_table->getProperty('ID_NUMBERING_ID_FIELD')) ? $t_related_table->get($vs_idno_field) : null,
									'description' => method_exists($t_related_table, 'getLabelForDisplay') ? $t_related_table->getLabelForDisplay() : '',
									'table_name' => $t_related_table->tableName(),
									'table_num' => $t_related_table->tableNum(),
									'row_id' => $t_related_table->getPrimaryKey(),
									'rel_type_id' => $va_log_entry['snapshot']['type_id'],
									'rel_typename' => $t_rel->getRelationshipTypename('ltor', $va_log_entry['snapshot']['type_id'])
								);
							}
						}
						// ---------------------------------------------------------------	
			
						// record log line
						if (sizeof($va_changes)) {
						    if ($vn_unit_id == '') {
						        $vs_unit_identifier = "U{$vn_pseudo_unit_counter}";
						        $vn_pseudo_unit_counter++;
						    } else {
						        $vs_unit_identifier = $vn_unit_id;
						    }
						
							$va_log_output[$vs_unit_identifier][] = array(
								'datetime' => $vs_datetime,
								'timestamp' => $va_log_entry['log_datetime'],
								'user_id' => $va_log_entry['user_id'],
								'user_fullname' => $vs_user,
								'user_email' => $vs_email,
								'user' => $vs_user.($vs_email ? ' ('.$vs_email.')' : ''),
								'changetype_display' => $va_change_types[$va_log_entry['changetype']],
								'changetype' => $va_log_entry['changetype'],
								'changes' => $va_changes,
								'subject' => $vs_subject_display_name,
								'subject_id' => $vn_subject_row_id,
								'subject_table_num' => $vn_subject_table_num,
								'logged_table_num' => $va_log_entry['logged_table_num'],
								'logged_table' => $t_obj->tableName(),
								'logged_row_id' => $va_log_entry['logged_row_id']
							);
						}
					}	
				}
			}
		}
		
		return $va_log_output;
	}
	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	public function getCreatedOnTimestampsForIDs($pm_table_name_or_num, $pa_row_ids) {
 		if (!is_array($pa_row_ids) || !sizeof($pa_row_ids)) { return array(); }
 		
 		if (!is_numeric($pm_table_name_or_num)) {
 			$pn_table_num = (int)Datamodel::getTableNum($pm_table_name_or_num);
 		} else {
 			$pn_table_num = (int)$pm_table_name_or_num;
 		}
 		
		$o_db = new Db();
		$qr_res = $o_db->query("
				SELECT wcl.log_datetime, wu.user_id, wu.fname, wu.lname, wu.email, wcl.logged_row_id
				FROM ca_change_log wcl
				LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
				WHERE
					(wcl.logged_table_num = ?) AND (wcl.logged_row_id IN (?)) AND(wcl.changetype = 'I')",
		$pn_table_num, $pa_row_ids);
		
		$va_timestamps = array();
		while ($qr_res->nextRow()) {
			$va_timestamps[$qr_res->get('logged_row_id')] = array(
				'user_id' => $qr_res->get('user_id'),
				'fname' => $qr_res->get('fname'),
				'lname' => $qr_res->get('lname'),
				'email' => $qr_res->get('email'),
				'timestamp' => $qr_res->get('log_datetime'),
				'date' => caGetLocalizedDate($qr_res->get('log_datetime'), ['timeOmit' => true]),
				'datetime' => caGetLocalizedDate($qr_res->get('log_datetime'))
			);
		}
 		
 		return $va_timestamps;
  	}
  	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	public function getLastChangeTimestampsForIDs($pm_table_name_or_num, $pa_row_ids) {
 		if (!is_array($pa_row_ids) || !sizeof($pa_row_ids)) { return array(); }
 		
 		if (!is_numeric($pm_table_name_or_num)) {
 			$pn_table_num = (int)Datamodel::getTableNum($pm_table_name_or_num);
 		} else {
 			$pn_table_num = (int)$pm_table_name_or_num;
 		}
 		
		$o_db = new Db();
		$va_timestamps = array();
		
		$qr_res = $o_db->query("
				SELECT wcl.log_datetime, wcls.subject_row_id, wu.user_id, wu.fname, wu.lname, wu.email
				FROM ca_change_log wcl
				INNER JOIN ca_change_log_subjects AS wcls ON wcl.log_id = wcls.log_id
				INNER JOIN
					(
						SELECT MAX(ch.log_datetime) log_datetime, sub.subject_row_id
						FROM ca_change_log ch
						INNER JOIN ca_change_log_subjects AS sub ON ch.log_id = sub.log_id
						WHERE
							(sub.subject_table_num = ?)
							AND
							(sub.subject_row_id IN (?))
							AND
							(ch.changetype IN ('I', 'U', 'D'))
						GROUP BY sub.subject_row_id
					) AS s ON s.subject_row_id = wcls.subject_row_id AND s.log_datetime = wcl.log_datetime
				LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
					
				",
		$pn_table_num, $pa_row_ids);
		
		while ($qr_res->nextRow()) {
			$va_timestamps[(int)$qr_res->get('subject_row_id')] = array(
				'user_id' => $qr_res->get('user_id'),
				'fname' => $qr_res->get('fname'),
				'lname' => $qr_res->get('lname'),
				'user' => $qr_res->get('fname').' '.$qr_res->get('lname'),
				'email' => $qr_res->get('email'),
				'timestamp' => $qr_res->get('log_datetime'),
				'date' => caGetLocalizedDate($qr_res->get('log_datetime'), ['timeOmit' => true]),
				'datetime' => caGetLocalizedDate($qr_res->get('log_datetime'))
			);
		}
		
		$qr_res = $o_db->query("
				SELECT wcl.log_datetime log_datetime, wcl.logged_row_id, wu.user_id, wu.fname, wu.lname, wu.email
				FROM ca_change_log wcl 
				INNER JOIN 
					(
						SELECT MAX(ch.log_datetime) log_datetime, ch.logged_row_id 
						FROM ca_change_log ch
						WHERE
							(ch.logged_table_num = ?)
							AND
							(ch.logged_row_id IN (?))
							AND
							(ch.changetype IN ('I', 'U', 'D'))
						GROUP BY ch.logged_row_id
					) AS s ON s.logged_row_id = wcl.logged_row_id AND s.log_datetime = wcl.log_datetime
				LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id",
		$pn_table_num, $pa_row_ids);
		
		while ($qr_res->nextRow()) {
			$vn_timestamp = (int)$qr_res->get('log_datetime');
			$vn_row_id = (int)$qr_res->get('logged_row_id');
			if ($vn_timestamp > $va_timestamps[$vn_row_id]) {
				 $va_timestamps[$vn_row_id] = array(
					'user_id' => $qr_res->get('user_id'),
					'fname' => $qr_res->get('fname'),
					'lname' => $qr_res->get('lname'),
					'user' => $qr_res->get('fname').' '.$qr_res->get('lname'),
					'email' => $qr_res->get('email'),
					'timestamp' => $qr_res->get('log_datetime'),
					'date' => caGetLocalizedDate($qr_res->get('log_datetime'), ['timeOmit' => true]),
					'datetime' => caGetLocalizedDate($qr_res->get('log_datetime'))
				);
			}
		}
		
 		
 		return $va_timestamps;
  	}
  	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	public function getDeleteOnTimestampsForIDs($pm_table_name_or_num, $pa_row_ids) {
 		if (!is_array($pa_row_ids) || !sizeof($pa_row_ids)) { return array(); }
 		
 		if (!is_numeric($pm_table_name_or_num)) {
 			$pn_table_num = (int)Datamodel::getTableNum($pm_table_name_or_num);
 		} else {
 			$pn_table_num = (int)$pm_table_name_or_num;
 		}
 		
		$o_db = new Db();
		$qr_res = $o_db->query("
				SELECT wcl.log_datetime, wu.user_id, wu.fname, wu.lname, wu.email, wcl.logged_row_id
				FROM ca_change_log wcl
				LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
				WHERE
					(wcl.logged_table_num = ?) AND (wcl.logged_row_id IN (?)) AND(wcl.changetype = 'D')",
		$pn_table_num, $pa_row_ids);
		
		$va_timestamps = array();
		while ($qr_res->nextRow()) {
			$va_timestamps[$qr_res->get('logged_row_id')] = array(
				'user_id' => $qr_res->get('user_id'),
				'fname' => $qr_res->get('fname'),
				'lname' => $qr_res->get('lname'),
				'user' => $qr_res->get('fname').' '.$qr_res->get('lname'),
				'email' => $qr_res->get('email'),
				'timestamp' => $qr_res->get('log_datetime')
			);
		}
 		
 		return $va_timestamps;
  	}
  	# ----------------------------------------------------------------------
 	/**
 	 *
 	 */
 	public function getEarliestTimestampForIDs($pm_table_name_or_num, $pa_row_ids=null) {
 		
 		if (!is_numeric($pm_table_name_or_num)) {
 			$pn_table_num = (int)Datamodel::getTableNum($pm_table_name_or_num);
 		} else {
 			$pn_table_num = (int)$pm_table_name_or_num;
 		}
 		
		$o_db = new Db();
		
		if (!is_array($pa_row_ids) || !sizeof($pa_row_ids)) {
			$qr_res = $o_db->query("
					SELECT MIN(wcl.log_datetime) log_datetime
					FROM ca_change_log wcl
					LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
					WHERE
						(wcl.logged_table_num = ?) AND (wcl.changetype = 'I')",
			$pn_table_num);
		} else {
			$qr_res = $o_db->query("
					SELECT MIN(wcl.log_datetime) log_datetime
					FROM ca_change_log wcl
					LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
					WHERE
						(wcl.logged_table_num = ?) AND (wcl.logged_row_id IN (?)) AND(wcl.changetype = 'I')",
			$pn_table_num, $pa_row_ids);
		}
		
		if ($qr_res->nextRow()) {
			return (int)$qr_res->get('log_datetime');
		}
 		
 		return null;
  	}
  	# ----------------------------------------------------------------------
 	/**
 	 * 
 	 *
 	 * @param mixed $pm_table_name_or_num
 	 * @param array $pa_options An array of options:
 	 * 		range = optional range to restrict returned entries to. Should be array with 0th key set to start and 1st key set to end of range. Both values should be Unix timestamps. You can also use 'start' and 'end' as keys if desired. 
	 * 		limit = maximum number of entries returned. Omit or set to zero for no limit. [default=all]
	 * @return array Change log data
 	 */
 	public function getDeletions($pm_table_name_or_num, $pa_options=null) {
 		$vn_table_num = Datamodel::getTableNum($pm_table_name_or_num);
 		$vs_table_name = Datamodel::getTableName($pm_table_name_or_num);
 		$t_subject = Datamodel::getInstanceByTableNum($vn_table_num, true);
 		
		$pa_datetime_range = (isset($pa_options['range']) && is_array($pa_options['range'])) ? $pa_options['range'] : null;
		$pn_max_num_entries_returned = (isset($pa_options['limit']) && (int)$pa_options['limit']) ? (int)$pa_options['limit'] : 0;
		
		if ($pa_datetime_range) {
			$vn_start = $vn_end = null;
			if (isset($pa_datetime_range[0])) {
				$vn_start = (int)$pa_datetime_range[0];
			} elseif (isset($pa_datetime_range['start'])) {
				$vn_start = (int)$pa_datetime_range['start'];
			}
			
			if (isset($pa_datetime_range[1])) {
				$vn_end = (int)$pa_datetime_range[1];
			} elseif (isset($pa_datetime_range['end'])) {
				$vn_end = (int)$pa_datetime_range['end'];
			}
			
			if ($vn_start <= 0) { $vn_start = time() - 3600; }
			if (!$vn_end <= 0) { $vn_end = time(); }
			if ($vn_end < $vn_start) { $vn_end = $vn_start; }
			
			
		} else {
			$vn_end = time();
			$vn_start = $vn_end - (24 * 60 * 60);
		}
		$o_db = new Db();
  		if (!($qr_res = $o_db->query("
			SELECT DISTINCT
				wcl.log_id, wcl.log_datetime log_datetime, wcl.user_id, wcl.changetype, wcl.logged_table_num, wcl.logged_row_id,
				wclsnap.snapshot, wcl.unit_id, wu.email, wu.fname, wu.lname
			FROM ca_change_log wcl
			INNER JOIN ca_change_log_snapshots AS wclsnap ON wclsnap.log_id = wcl.log_id
			LEFT JOIN ca_change_log_subjects AS wcls ON wcl.log_id = wcls.log_id
			LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
			WHERE
				(
					(wcl.logged_table_num = ".(int)$vn_table_num.") AND (wcl.changetype = 'D') 
				)
				AND (wcl.log_datetime > ? AND wcl.log_datetime < ?)
			ORDER BY wcl.log_datetime
		", $vn_start, $vn_end))) {
			# should not happen
			return false;
		}
		
		$va_log = array();
		while($qr_res->nextRow()) {
			$va_log[$vn_row_id = (int)$qr_res->get('logged_row_id')] = array(
				'datetime' => date("n/d/Y@g:i:sa T", $qr_res->get('log_datetime')),
				'table' => $vs_table_name,
				'row_id' => $vn_row_id
			);
		}
		
		if ($qr_res = $t_subject->makeSearchResult($vs_table_name, array_keys($va_log))) {
			$vs_pk = $t_subject->primaryKey();
			while($qr_res->nextHit()) {
				if (!($vn_row_id = $qr_res->get("{$vs_table_name}.{$vs_pk}"))) { continue; }
				$va_log[$vn_row_id]['label'] = $qr_res->get("{$vs_table_name}.preferred_labels");
				$va_log[$vn_row_id]['idno'] = $qr_res->get("{$vs_table_name}.idno");
			}
		}
		return $va_log;
	}
 	# ----------------------------------------------------------------------
 	# New API
 	# ----------------------------------------------------------------------
 	/**
 	 *
 	 * @param array $pa_options An array of options:
 	 * 		daterange = 
	 * 		limit = maximum number of entries returned. Omit or set to zero for no limit. [default=all]
 	 *		transaction =
 	 */
 	static public function getChangeLogUsers($options=null) {
		$o_db = ($trans = caGetOption('transaction', $options, null)) ? $trans->getDb() : new Db();
 		
 		$params = [];
 		$daterange_values = ($daterange = caGetOption('daterange', $options, null)) ? caDateToUnixTimestamps($daterange) : null;
 		
 		$wheres = [];
 		if (is_array($daterange_values)) {
 			$wheres[] = "AND (wcl.log_datetime > ? AND wcl.log_datetime < ?)";
 			$params[] = $daterange_values['start']; $params[] = $daterange_values['end'];
 		}
 		
 		$sql_wheres = sizeof($wheres) ? "WHERE ".join(" AND ", $wheres) : '';
 		if (!($qr = $o_db->query("
			SELECT DISTINCT
				wcl.user_id, wu.email, wu.fname, wu.lname, wu.user_name
			FROM ca_change_log wcl
			LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
				{$sql_wheres}
				
			ORDER BY wu.lname, wu.fname
		", $params))) {
			# should not happen
			return false;
		}
		
		$users = [];
		while($qr->nextRow()) {
			$row = $qr->getRow();
			if (!$qr->get('user_name') || !($qr->get('lname') || $qr->get('fname'))) { continue; }
			$email = $qr->get('email');
			$row['user'] = $qr->get('fname').' '.$qr->get('lname').' '.($email ? "({$email})" : '');
			
			$users[] = $row;
		}
		return $users;
 	}
 	# ----------------------------------------------------------------------
 	/**
 	 *
 	 * @param array $pa_options An array of options:
 	 * 		daterange = 
 	 *		format = 
 	 *		start =
	 * 		limit = Maximum number of entries returned. Omit or set to zero for no limit. [default=all]
 	 *		transaction =
 	 *
 	 * @return array
 	 */
 	static public function getChangeLogUsersForSelect($options=null) {
 		if (is_array($users = self::getChangeLogUsers($options))) {
 			$format = caGetOption('format', $options, "^user");
 			$opts = [];
 			foreach($users as $u) {
 				$opts[caProcessTemplate($format, $u)] = $u['user_id'];
 			}
 			return $opts;
 		}
 		return null;
 	}
 	# ----------------------------------------------------------------------
 	/**
 	 * 
 	 *
 	 * @param mixed $table Table name or number
 	 * @param array $options Options include:
 	 *		table = 
 	 *		daterange = 
 	 *		user_id = 
 	 *		start =
 	 *		limit = Maximum number of entries returned. Omit or set to zero for no limit. [default=all]
 	 *		transaction =
 	 *		noSnapshot = 
 	 *
 	 * @return array
 	 */
 	static public function getChangeLog($options=null) {
 		$data = self::getChangeData($options);
 		return self::makeChangeLog($data, $table, $options);
 	}
 	# ----------------------------------------------------------------------
 	/**
 	 * 
 	 *
 	 * @param mixed $table Table name or number
 	 * @param array $options Options include:
 	 *		table = s
 	 *		daterange = 
 	 *		user_id = 
 	 *		start =
 	 *		limit = Maximum number of entries returned. Omit or set to zero for no limit. [default=all]
 	 *		transaction =
 	 *		noSnapshot = 
 	 *
 	 * @return array
 	 */
 	static public function getChangeData($options=null) {
		$o_db = ($trans = caGetOption('transaction', $options, null)) ? $trans->getDb() : new Db();
		
		$table = caGetOption('table', $options, null);
		if ($table && !($table_name = Datamodel::getTableName($table))) { return null; }
		$table_num = $table_name ? Datamodel::getTableNum($table_name) : null;
		
		$params = [];
		
		$start = caGetOption('start', $options, 0, ['castTo' => 'int']);
		$limit = caGetOption('limit', $options, null, ['castTo' => 'int']);
		$user_id = caGetOption('user_id', $options, null);
		
		$sql_limit = ($limit > 0) ? "LIMIT {$start},{$limit}" : '';
		
		$no_snapshot = caGetOption('noSnapshot', $options, false, ['castTo' => 'bool']);
		
		$sql_table = null;
		if ($table_num) {
			$sql_tables = ['((wcl.logged_table_num = ?) AND (wcls.subject_table_num IS NULL))', '(wcls.subject_table_num = ?)'];
			$params[] = $table_num;
		}
		
		$sql_daterange = null;
		if (($daterange = caGetOption('daterange', $options, null)) && is_array($d = caDateToUnixTimestamps($daterange))) {
			$sql_daterange = "AND (wcl.log_datetime BETWEEN ? AND ?)";
			$params[] = $d['start']; $params[] = $d['end'];
		}
		
		$sql_user_id = null;
		if($user_id) {
			if (!is_array($user_id)) { $user_id = [$user_id]; }
			if (sizeof($user_id = array_filter(array_map(function($v) { return (int)$v; }, $user_id), function($v) { return ($v > 0); })) > 0) {
		    	$sql_user_id = "AND (wcl.user_id IN (?))";
				$params[] = $user_id;
			}
		}
		
		if (!is_array($sql_tables)) { $sql_tables[] = "1"; }
		
		$log = [];
		foreach($sql_tables as $sql_table) {
			$qr = $o_db->query($z="
				SELECT
					wcl.log_id, wcl.log_datetime log_datetime, wcl.user_id, wcl.changetype, wcl.logged_table_num, wcl.logged_row_id,
					 wcl.unit_id, wu.email, wu.fname, wu.lname, wcls.subject_table_num, wcls.subject_row_id ".($no_snapshot ? '' : ', wclsnap.snapshot')."
				FROM ca_change_log wcl
				INNER JOIN ca_change_log_snapshots AS wclsnap ON wclsnap.log_id = wcl.log_id
				LEFT JOIN ca_change_log_subjects AS wcls ON wcl.log_id = wcls.log_id
				LEFT JOIN ca_users AS wu ON wcl.user_id = wu.user_id
				WHERE
					{$sql_table} {$sql_daterange} {$sql_user_id}
				{$sql_limit}
			", $params);
			if ($qr) {
				while($qr->nextRow()) {
					$row = $qr->getRow();
				
					if (!$no_snapshot) { 
						$row['snapshot'] = caUnserializeForDatabase($row['snapshot']);
					}
					$log[$row['log_id']][] = $row;
				}
			}
		}
		ksort($log);
		$log = array_reduce($log, function($c, $i) { foreach($i as $v) { if(is_array($v)) { $c[] = $v; }} return $c; }, []);
		return array_reverse($log);
 	}
 	# ----------------------------------------
	/**
 	 *
 	 * @param array $options Options include:
 	 *		dontShowTimestampInChangeLog = 
 	 *		returnItemNames = 
 	 *
 	 * @return array
 	 */
	static private function makeChangeLog($data, $options=null) {
		$dont_show_timestamp_in_change_log = caGetOption('dontShowTimestampInChangeLog', $options, false);
		$return_item_names = caGetOption('returnItemNames', $options, true);
		
		$va_log_output = array();
		$vs_blank_placeholder = '&lt;'._t('BLANK').'&gt;';
		$o_tep = new TimeExpressionParser();
		
		if (!$options) { $options = array(); }
		$t_user = ($user_id = caGetOption('user_id', $options, null)) ? new ca_users($user_id) : null;
		
		if (sizeof($data)) {
			//
			// Init
			//
			$va_change_types = array(
				'I' => _t('Added'),
				'U' => _t('Edited'),
				'D' => _t('Deleted')
			);
			
			
			
			//
			// Group data by unit
			//
			$va_grouped_data = array();
			foreach($data as $va_log_entry) {
				$va_grouped_data[$va_log_entry['unit_id']]['ca_table_num_'.$va_log_entry['logged_table_num']][] = $va_log_entry;
			}
			
			//
			// Process units
			//
			$va_attributes = array();
			$vn_pseudo_unit_counter = 1;
			foreach($va_grouped_data as $vn_unit_id => $va_log_entries_by_table) {
				foreach($va_log_entries_by_table as $vs_table_key => $va_log_entries) {
					foreach($va_log_entries as $va_log_entry) {
						$vs_label_table_name = $vs_label_display_name = '';
						$t_item = Datamodel::getInstanceByTableNum($va_log_entry['logged_table_num'], true);
			
						$vs_label_table_name = $vn_label_table_num = $vs_label_display_name = null;
						if (method_exists($t_item, 'getLabelTableName') && $t_item->getLabelTableInstance()) {
							$t_item_label = $t_item->getLabelTableInstance();
							$vs_label_table_name = $t_item->getLabelTableName();
							$vn_label_table_num = $t_item_label->tableNum();
							$vs_label_display_name = $t_item_label->getProperty('NAME_SINGULAR');
						}
			
						$va_changes = array();
						
						if (!is_array($va_log_entry['snapshot'])) { $va_log_entry['snapshot'] = array(); }
						
						//
						// Get date/time stamp for display
						//
						$o_tep->setUnixTimestamps($va_log_entry['log_datetime'], $va_log_entry['log_datetime']);
						if($dont_show_timestamp_in_change_log) {
							$vs_datetime = $o_tep->getText(array('timeOmit' => true));
						} else {
							$vs_datetime = $o_tep->getText();
						}
						
						//
						// Get user name
						//
						$vs_user = $va_log_entry['fname'].' '.$va_log_entry['lname'];
						$vs_email = $va_log_entry['email'];
						
						// The "logged" table/row is the row to which the change was actually applied
						// The "subject" table/row is the row to which the change is considered to have been made for workflow purposes.
						//
						// For example: if an entity is related to an object, strictly speaking the logging occurs on the ca_objects_x_entities
						// row (ca_objects_x_entities is the "logged" table), but the subject is ca_objects since it's only in the context of the
						// object (and probably the ca_entities row as well) that you can about the change.
						//		
						$t_obj = Datamodel::getInstanceByTableNum($va_log_entry['logged_table_num'], true);	// get instance for logged table
						if (!$t_obj) { continue; }
						
						$vs_subject_display_name = _t('&lt;MISSING&gt;');
						$vn_subject_row_id = null;
						$vn_subject_table_num = null;
						if ($return_item_names) {
							if (!($vn_subject_table_num = $va_log_entry['subject_table_num'])) {
								$vn_subject_table_num = $va_log_entry['logged_table_num'];
								$vn_subject_row_id = $va_log_entry['logged_row_id'];
							} else {
								$vn_subject_row_id = $va_log_entry['subject_row_id'];
							}
							
							if ($t_subject = Datamodel::getInstanceByTableNum($vn_subject_table_num, true)) {
								if ($t_subject->load($vn_subject_row_id)) {
									if (method_exists($t_subject, 'getLabelForDisplay')) {
										$vs_subject_display_name = $t_subject->getLabelForDisplay(false);
									} else {
										if ($vs_idno_field = $t_subject->getProperty('ID_NUMBERING_ID_FIELD')) {
											$vs_subject_display_name = $t_subject->getProperty('NAME_SINGULAR').' ['.$t_subject->get($vs_idno_field).']';
										} else {
											$vs_subject_display_name = $t_subject->getProperty('NAME_SINGULAR').' ['.$vn_subject_row_id.']';
										}
									}
								}
							}
						}
						
						//
						// Get item changes
						//
						
						// ---------------------------------------------------------------
						// is this an intrinsic field?
						if (($table_num == $va_log_entry['logged_table_num'])) {
							foreach($va_log_entry['snapshot'] as $vs_field => $vs_value) {
								$va_field_info = $t_obj->getFieldInfo($vs_field);
								if (isset($va_field_info['IDENTITY']) && $va_field_info['IDENTITY']) { continue; }
								if (isset($va_field_info['DISPLAY_TYPE']) && $va_field_info['DISPLAY_TYPE'] == DT_OMIT) { continue; }
								if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $vs_field)) { continue; }	// does user have access to this bundle?
								
								if ((isset($va_field_info['DISPLAY_FIELD'])) && (is_array($va_field_info['DISPLAY_FIELD'])) && ($va_disp_fields = $va_field_info['DISPLAY_FIELD'])) {
									//
									// Lookup value in related table
									//
									if (!$vs_value) { continue; }
									if (sizeof($va_disp_fields)) {
										$va_rel = Datamodel::getManyToOneRelations($t_obj->tableName(), $vs_field);
										$va_rel_values = array();
											
										if ($t_rel_obj = Datamodel::getInstance($va_rel['one_table'], true)) {
											$t_rel_obj->load($vs_value);
											
											foreach($va_disp_fields as $vs_display_field) {
												$va_tmp = explode('.', $vs_display_field);
												if (($vs_tmp = $t_rel_obj->get($va_tmp[1])) !== '') { $va_rel_values[] = $vs_tmp; }
											}
										}	
										$vs_proc_val = join(', ', $va_rel_values);
									}
								} else {
									// Is field a foreign key?
									$va_keys = Datamodel::getManyToOneRelations($t_obj->tableName(), $vs_field);
									if (sizeof($va_keys)) {
										// yep, it's a foreign key
										$va_rel_values = array();
										
										if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $va_keys['one_table'])) { continue; }	// does user have access to this bundle?
								
										if ($t_rel_obj = Datamodel::getInstance($va_keys['one_table'], true)) {
											if ($t_rel_obj->load($vs_value)) {
												if (method_exists($t_rel_obj, 'getLabelForDisplay')) {
													$vs_proc_val = $t_rel_obj->getLabelForDisplay(false);
												} else {
													$va_disp_fields = $t_rel_obj->getProperty('LIST_FIELDS');
													foreach($va_disp_fields as $vs_display_field) {
														if (($vs_tmp = $t_rel_obj->get($vs_display_field)) !== '') { $va_rel_values[] = $vs_tmp; }
													}
													$vs_proc_val = join(' ', $va_rel_values);
												}
												if (!$vs_proc_val) { $vs_proc_val = _t('&lt;MISSING&gt;'); }
											} else {
												$vs_proc_val = _t("Not set");
											}
										} else {
											$vs_proc_val = _t('Non-existent');
										}
									} else {
							
										// Adjust display of value for different field types
										switch($va_field_info['FIELD_TYPE']) {
											case FT_BIT:
												$vs_proc_val = $vs_value ? 'Yes' : 'No';
												break;
											default:
												$vs_proc_val = $vs_value;
												break;
										}
										
										if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $vs_field)) { continue; }	// does user have access to this bundle?
										
										// Adjust display of value for lists
										if ($va_field_info['LIST']) {
											$t_list = new ca_lists();
											if ($t_list->load(array('list_code' => $va_field_info['LIST']))) {
												$vn_list_id = $t_list->getPrimaryKey();
												$t_list_item = new ca_list_items();
												if ($t_list_item->load(array('list_id' => $vn_list_id, 'item_value' => $vs_value))) {
													$vs_proc_val = $t_list_item->getLabelForDisplay();
												}
											}
										} else {
											if ($va_field_info['BOUNDS_CHOICE_LIST']) {
												// TODO
											}
										}
									}
								}
								
								$va_changes[] = array(
									'label' => $va_field_info['LABEL'],
									'description' => (strlen((string)$vs_proc_val) ? $vs_proc_val : $vs_blank_placeholder),
									'value' => $vs_value
								);
							}
						}
													
						// ---------------------------------------------------------------
						// is this a label row?
						if ($va_log_entry['logged_table_num'] == $vn_label_table_num) {
							
							foreach($va_log_entry['snapshot'] as $vs_field => $vs_value) {
								$va_changes[] = array(
									'label' => $t_item_label->getFieldInfo($vs_field, 'LABEL'),
									'description' => $vs_value
								);
							}
						}
						
						// ---------------------------------------------------------------
						// is this an attribute?
						if ($va_log_entry['logged_table_num'] == 3) {	// attribute_values
							if ($t_element = ca_attributes::getElementInstance($va_log_entry['snapshot']['element_id'])) {
								
								if ($t_element->get('parent_id') && ($t_container = ca_attributes::getElementInstance($t_element->get('hier_element_id')))) {
									$vs_element_code = $t_container->get('element_code');
								} else {
									$vs_element_code = $t_element->get('element_code');
								}
								
								if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $vs_element_code)) { continue; }	// does user have access to this bundle?
							
								if ($o_attr_val = Attribute::getValueInstance($t_element->get('datatype'))) {
									$o_attr_val->loadValueFromRow($va_log_entry['snapshot']);
									$vs_attr_val = $o_attr_val->getDisplayValue();
								} else {
									$vs_attr_val = '?';
								}
								
								// Convert list-based attributes to text
								if ($vn_list_id = $t_element->get('list_id')) {
									$t_list = new ca_lists();
									$vs_attr_val = $t_list->getItemFromListForDisplayByItemID($vn_list_id, $vs_attr_val, true);
								}
								
								if (!$vs_attr_val) { 
									$vs_attr_val = $vs_blank_placeholder;
								}
								$vs_label = $t_element->getLabelForDisplay();
								$va_attributes[$va_log_entry['snapshot']['attribute_id']]['values'][] = array(
									'label' => $vs_label,
									'value' => $vs_attr_val
								);
								$va_changes[] = array(
									'label' => $vs_label,
									'description' => $vs_attr_val
								);
							}
						}
						
						// ---------------------------------------------------------------
						// is this a related (many-many) row?
						$va_keys = Datamodel::getOneToManyRelations($t_item->tableName(), $t_obj->tableName());
						if (sizeof($va_keys) > 0) {
							if (method_exists($t_obj, 'getLeftTableNum')) {
								if ($t_obj->getLeftTableNum() == $t_item->tableNum()) {
									// other side of rel is on right
									$t_related_table = Datamodel::getInstanceByTableNum($t_obj->getRightTableNum(), true);
									$t_related_table->load($va_log_entry['snapshot'][$t_obj->getRightTableFieldName()]);
								} else {
									// other side of rel is on left
									$t_related_table = Datamodel::getInstanceByTableNum($t_obj->getLeftTableNum(), true);
									$t_related_table->load($va_log_entry['snapshot'][$t_obj->getLeftTableFieldName()]);
								}
								$t_rel = Datamodel::getInstanceByTableNum($t_obj->tableNum(), true);
								
								if ($t_user && !$t_user->getBundleAccessLevel($t_item->tableName(), $t_related_table->tableName())) { continue; }	// does user have access to this bundle?
							
								$va_changes[] = array(
									'label' => caUcFirstUTF8Safe($t_related_table->getProperty('NAME_SINGULAR')),
									'idno' => ($vs_idno_field = $t_related_table->getProperty('ID_NUMBERING_ID_FIELD')) ? $t_related_table->get($vs_idno_field) : null,
									'description' => method_exists($t_related_table, 'getLabelForDisplay') ? $t_related_table->getLabelForDisplay() : '',
									'table_name' => $t_related_table->tableName(),
									'table_num' => $t_related_table->tableNum(),
									'row_id' => $t_related_table->getPrimaryKey(),
									'rel_type_id' => $va_log_entry['snapshot']['type_id'],
									'rel_typename' => $t_rel->getRelationshipTypename('ltor', $va_log_entry['snapshot']['type_id'])
								);
							}
						}
						// ---------------------------------------------------------------	
			
						// record log line
						if (sizeof($va_changes)) {
						    if ($vn_unit_id == '') {
						        $vs_unit_identifier = "U{$vn_pseudo_unit_counter}";
						        $vn_pseudo_unit_counter++;
						    } else {
						        $vs_unit_identifier = $vn_unit_id;
						    }
						
							$va_log_output[$vs_unit_identifier][] = array(
								'datetime' => $vs_datetime,
								'timestamp' => $va_log_entry['log_datetime'],
								'user_id' => $va_log_entry['user_id'],
								'user_fullname' => $vs_user,
								'user_email' => $vs_email,
								'user' => $vs_user.($vs_email ? ' ('.$vs_email.')' : ''),
								'changetype_display' => $va_change_types[$va_log_entry['changetype']],
								'changetype' => $va_log_entry['changetype'],
								'changes' => $va_changes,
								'subject' => $vs_subject_display_name,
								'subject_id' => $vn_subject_row_id,
								'subject_table_num' => $vn_subject_table_num,
								'logged_table_num' => $va_log_entry['logged_table_num'],
								'logged_table' => $t_obj->tableName(),
								'logged_row_id' => $va_log_entry['logged_row_id']
							);
						}
					}	
				}
			}
		}
		
		return $va_log_output;
	}
 	# ----------------------------------------------------------------------
 }
