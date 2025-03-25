extends Camera3D

@export var sensitivity: float = 0.3  # Mouse rotation sensitivity
@export var pan_speed: float = 0.5  # Speed for panning (middle mouse drag)
@export var zoom_speed: float = 2.0  # Zoom speed
@export var min_zoom: float = 2.0  # Minimum zoom distance
@export var max_zoom: float = 50.0  # Maximum zoom distance

var _mouse_delta: Vector2 = Vector2.ZERO
var _total_pitch: float = 0.0
var _is_rotating: bool = false
var _is_panning: bool = false

func _input(event):
	# Mouse movement
	if event is InputEventMouseMotion:
		_mouse_delta = event.relative

		# Rotate camera (right mouse button)
		if _is_rotating:
			rotate_camera()

		# Pan camera (middle mouse button)
		elif _is_panning:
			pan_camera()
	
	# Mouse button input
	if event is InputEventMouseButton:
		match event.button_index:
			MOUSE_BUTTON_RIGHT:  # Rotate when right-click is held
				_is_rotating = event.pressed
			MOUSE_BUTTON_MIDDLE:  # Pan when middle-click is held
				_is_panning = event.pressed
			MOUSE_BUTTON_WHEEL_UP:  # Zoom in
				zoom_camera(-zoom_speed)
			MOUSE_BUTTON_WHEEL_DOWN:  # Zoom out
				zoom_camera(zoom_speed)

func rotate_camera():
	_mouse_delta *= sensitivity
	var yaw = -_mouse_delta.x
	var pitch = -_mouse_delta.y
	_mouse_delta = Vector2.ZERO  # Reset delta after applying movement

	# Prevent excessive vertical rotation
	_total_pitch = clamp(_total_pitch + pitch, -89, 89)
	
	rotate_y(deg_to_rad(yaw))
	rotation_degrees.x = _total_pitch  # Directly set pitch to prevent accumulation errors

func pan_camera():
	# Moves the camera left/right/up/down using middle mouse drag
	var pan_x = -_mouse_delta.x * pan_speed
	var pan_y = _mouse_delta.y * pan_speed
	_mouse_delta = Vector2.ZERO

	translate(Vector3(pan_x, pan_y, 0))  # Move in world space

func zoom_camera(amount: float):
	# Moves the camera forward/backward for zoom
	var zoom_offset = -global_transform.basis.z * amount
	var new_position = position + zoom_offset
	position = new_position
