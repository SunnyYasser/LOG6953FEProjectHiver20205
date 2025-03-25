extends Node3D

@export var city_size: int = 4  # Number of city blocks in each direction
@export var block_size: int = 16  # Number of buildings per block (excluding roads)
@export var tile_size: float = 10.0  # Distance between buildings
@export var road_width: float = 2.0  # Width of the road
@export var road_scene: PackedScene = preload("res://road.tscn")
@export var building_scene: PackedScene = preload("res://building.tscn")

# Seed for consistent random generation
@export var city_seed: int = 42

var building_id_counter: int = 0  # Unique ID counter

func _ready():
	seed(city_seed)
	generate_city()
	Input.set_mouse_mode(Input.MOUSE_MODE_VISIBLE)  # Ensure the cursor is visible

func generate_city():
	for bx in range(city_size):
		for by in range(city_size):
			var block_origin = Vector3(
				bx * (block_size + 1) * tile_size, 
				0, 
				by * (block_size + 1) * tile_size
			)
			generate_block(block_origin)

func generate_block(origin: Vector3):
	var building_positions = calculate_building_positions(origin)
	place_block_roads(origin)
	
	for pos in building_positions:
		place_building(pos)

func calculate_building_positions(origin: Vector3) -> Array:
	var positions = []
	for x in range(block_size):
		for y in range(block_size):
			var pos = origin + Vector3(x * tile_size, 0, y * tile_size)
			positions.append(pos)
	return positions

func place_block_roads(origin: Vector3):
	for x in range(block_size + 1):
		var road_pos = origin + Vector3(x * tile_size, 0, block_size * tile_size)
		place_horizontal_road(road_pos)
	for y in range(block_size + 1):
		var road_pos = origin + Vector3(block_size * tile_size, 0, y * tile_size)
		place_vertical_road(road_pos)

func place_horizontal_road(pos: Vector3):
	var road = road_scene.instantiate()
	road.position = pos
	road.rotation.y = 0  
	road.scale = Vector3(block_size + 1, 0.2, road_width)
	add_child(road)

func place_vertical_road(pos: Vector3):
	var road = road_scene.instantiate()
	road.position = pos
	road.rotation.y = PI / 2  
	road.scale = Vector3(block_size + 1, 0.2, road_width)
	add_child(road)

func place_building(pos: Vector3):
	var building = building_scene.instantiate()
	building.position = pos
	building.set_meta("building_id", building_id_counter)  # Assign a unique ID
	building_id_counter += 1  # Increment for next building
	
	# Enable input detection for clicking
	building.set_script(preload("res://building_script.gd"))
	
	add_child(building)

func _input(event):
	if event is InputEventMouseButton and event.pressed and event.button_index == MOUSE_BUTTON_LEFT:
		check_building_click()

func check_building_click():
	var space_state = get_world_3d().direct_space_state
	var mouse_pos = get_viewport().get_mouse_position()
	var camera = get_viewport().get_camera_3d()
	var from = camera.project_ray_origin(mouse_pos)
	var to = from + camera.project_ray_normal(mouse_pos) * 1000  # Long ray

	var query = PhysicsRayQueryParameters3D.create(from, to)
	var result = space_state.intersect_ray(query)

	if result and result.has("collider"):
		var clicked_building = result["collider"]
		if clicked_building.has_meta("building_id"):
			print("Building ID:", clicked_building.get_meta("building_id"))
