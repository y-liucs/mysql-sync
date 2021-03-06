[
		{
			"dbName" : "uic",
			"tables" : [
					{
						"tableName" : "uic_user",
						"update" : true,
						"ignores" : [ "loginname", "password", "phone",
								"head_img", "third_head_img" ]
					}, {
						"tableName" : "uic_burying_point",
						"update" : false
					}, {
						"tableName" : "uic_user_device",
						"update" : true
					}, {
						"tableName" : "uic_user_log",
						"update" : false
					}, {
						"tableName" : "uic_group_privilege",
						"update" : true
					}, {
						"tableName" : "uic_group_type",
						"update" : true
					}, {
						"tableName" : "uic_privilege",
						"update" : true
					}, {
						"tableName" : "uic_group",
						"update" : false
					}, {
						"tableName" : "uic_download_log_info",
						"update" : false
					} ]
		},
		{
			"dbName" : "trans",
			"tables" : [
					{
						"tableName" : "plat_notice",
						"update" : true
					},
					{
						"tableName" : "trans_account",
						"update" : true
					},
					{
						"tableName" : "trans_activity_convert",
						"update" : true
					},
					{
						"tableName" : "plat_task",
						"update" : true
					},
					{
						"tableName" : "plat_task_awards",
						"update" : true
					},
					{
						"tableName" : "trans_change_note",
						"update" : false
					},
					{
						"tableName" : "trans_convert",
						"update" : false
					},
					{
						"tableName" : "trans_fragment_account",
						"update" : false
					},
					{
						"tableName" : "trans_fragment_change_log",
						"update" : false
					},
					{
						"tableName" : "trans_fragment_convert_record",
						"update" : false
					},
					{
						"tableName" : "trans_awards_in_out",
						"update" : false
					},
					{
						"tableName" : "trans_handsel_card",
						"update" : false
					},
					{
						"tableName" : "quoits_weight_config",
						"update" : false
					},
					{
						"tableName" : "quoits_awards_info",
						"update" : false
					},
					{
						"tableName" : "plat_wheel_weight_config",
						"update" : true
					},
					{
						"tableName" : "plat_profit_awards_info",
						"update" : false
					},
					{
						"tableName" : "inventory_activity_info",
						"update" : false
					},
					{
						"tableName" : "trans_fragment",
						"update" : false
					},
					{
						"tableName" : "inventory_channel_info",
						"update" : false
					},
					{
						"tableName" : "inventory_phy_awards_info",
						"update" : true
					},
					{
						"tableName" : "plat_profit_awards_info",
						"update" : true
					},
					{
						"tableName" : "plat_profit_awards_config",
						"update" : true
					},
					{
						"tableName" : "trans_product",
						"update" : true
					},
					{
						"tableName" : "trans_product_channel",
						"update" : true
					},
					{
						"tableName" : "app_help_center",
						"update" : true
					},
					{
						"tableName" : "app_index_window_info",
						"update" : true
					},
					{
						"tableName" : "plat_navigator",
						"update" : true
					},
					{
						"tableName" : "plat_weekcard",
						"update" : true
					},
					{
						"tableName" : "plat_welfare",
						"update" : true
					},
					{
						"tableName" : "plat_banner",
						"update" : true
					},
					{
						"tableName" : "plat_game",
						"update" : true
					},
					{
						"tableName" : "plat_version_config",
						"update" : true
					},
					{
						"tableName" : "plat_wheel_user_play_record",
						"update" : false,
						"ignores" : [ "receive_remark", "receiver_name",
								"receiver_mobile", "receiver_address" ]
					},
					{
						"tableName" : "plat_ranking_history",
						"update" : false,
						"ignores" : [ "receive_remark", "receiver_name",
								"receiver_mobile", "receiver_address" ]
					},
					{
						"tableName" : "quoits_user_play_record",
						"update" : false,
						"ignores" : [ "receive_remark", "receiver_name",
								"receiver_mobile", "receiver_address" ]
					},
					{
						"tableName" : "weituo_recharge_user",
						"update" : true
					},
					{
						"tableName" : "inventory_phy_awards_sendlog",
						"update" : true,
						"ignores" : [ "receive_remark", "receiver_name",
								"receiver_mobile", "receiver_address" ]
					}, {
						"tableName" : "plat_luckey_packet_record",
						"update" : false
					}, {
						"tableName" : "plat_signed_user",
						"update" : false
					}, {
						"tableName" : "trans_phone_flow_recharge",
						"update" : true
					}, {
						"tableName" : "trans_phone_recharge",
						"update" : true
					}, ]
		}, {
			"dbName" : "report",
			"tables" : [ {
				"tableName" : "report_change_note",
				"update" : false
			} ]
		},  {
			"dbName" : "system",
			"tables" : [ {
				"tableName" : "sys_menu",
				"update" : true
			}, {
				"tableName" : "sys_config",
				"update" : true
			}, {
				"tableName" : "sys_dict",
				"update" : true
			}, {
				"tableName" : "sys_task",
				"update" : true
			}    ]
		},{
			"dbName" : "mall",
			"tables" : [ {
				"tableName" : "mall_awards_config",
				"update" : true
			}, {
				"tableName" : "mall_awards_info",
				"update" : true
			}, {
				"tableName" : "mall_biz_config",
				"update" : true
			}, {
				"tableName" : "mall_biz_log",
				"update" : true
			}, {
				"tableName" : "mall_channel_payment",
				"update" : true
			}, {
				"tableName" : "mall_payment_mode",
				"update" : true
			}, {
				"tableName" : "mall_product_card",
				"update" : true
			}, {
				"tableName" : "mall_product_common",
				"update" : true
			}, {
				"tableName" : "mall_product_welfare",
				"update" : true
			}, {
				"tableName" : "mall_weekcard_log",
				"update" : true
			}, {
				"tableName" : "mall_welfare_log",
				"update" : true
			} ]
		} ]
