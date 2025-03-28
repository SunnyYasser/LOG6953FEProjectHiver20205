extends StaticBody3D

@onready var rmq_connection = get_tree().get_root().find_child("rmq_listener", true, false)
@onready var rmq_listener = get_node("/root/Node3D/rmq_listener")

func _input_event(camera, event, position, normal, shape_idx):
	if event is InputEventMouseButton and event.pressed and event.button_index == MOUSE_BUTTON_LEFT:
		if has_meta("building_id"):
			var building_id = get_meta("building_id")
			print("Clicked Building ID:", building_id)
			_send_message(building_id)

func _send_message(building_id: int):
	var message = {
		"event": "building_clicked",
		"building_id": building_id
	}
	var json_message = JSON.stringify(message)
	
	if rmq_connection:
		rmq_listener.Publish(json_message)
		pass
	else:
		print("Error: RabbitMQListener not found!")
