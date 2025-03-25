using Godot;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

public partial class RabbitMQListener : Node
{
	private ConnectionFactory factory = new ConnectionFactory();
	private IConnection connection;
	private IModel channel;

	private string exchangeName = "BuildingSimulation";  // 🔹 Changed for new project
	private string ROUTING_KEY_BUILDING_CLICK = "building.record.click";  // 🔹 New routing keys
	private string ROUTING_KEY_BUILDING_UPDATE = "building.record.update";

	private string userName = "guest";  
	private string hostName = "localhost";
	private string password = "guest";  
	private string port = "5672";

	private string localQueue;
	private List<string> messages = new();

	[Signal]
	public delegate void OnMessageEventHandler(string message);

	public override void _Ready()
	{
		factory.UserName = userName;
		factory.Password = password;
		factory.HostName = hostName;
		factory.Port = port.ToInt();

		connection = factory.CreateConnection();
		channel = connection.CreateModel();
		
		localQueue = channel.QueueDeclare(autoDelete: true, exclusive: true);
		channel.QueueBind(queue: localQueue, exchange: exchangeName, routingKey: ROUTING_KEY_BUILDING_CLICK);
		channel.QueueBind(queue: localQueue, exchange: exchangeName, routingKey: ROUTING_KEY_BUILDING_UPDATE);
		ReceiveMessage();
		
		GD.Print(connection.IsOpen ? "✅ Connection established" : "❌ Error! Could not connect!");
	}

	public override void _Process(double delta)
	{
		for (int i = 0; i < messages.Count; i++)
		{
			EmitSignal(SignalName.OnMessage, messages[i]);
		}
		messages.Clear();
	}

	private void ReceiveMessage()
	{
		GD.Print("📡 Waiting for messages...");
		var consumer = new EventingBasicConsumer(channel);

		consumer.Received += (model, ea) =>
		{
			var body = ea.Body.ToArray();
			var message = Encoding.UTF8.GetString(body);
			messages.Add(message);
		};

		channel.BasicConsume(queue: localQueue, autoAck: true, consumer: consumer);
	}

	private void Publish(string message)
	{
		if (channel == null || !connection.IsOpen)
		{
			GD.PrintErr("❌ RabbitMQ connection is closed! Cannot publish.");
			return;
		}

		var body = Encoding.UTF8.GetBytes(message);
		channel.BasicPublish(exchange: exchangeName, routingKey: "building.clicked", basicProperties: null, body: body);
		GD.Print("📩 Sent message to RabbitMQ:", message);
	}

	public override void _ExitTree()
	{
		channel?.Close();
		connection?.Close();
	}
}
