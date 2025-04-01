using Godot;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json; // Make sure to install JSON.NET (Newtonsoft.Json) via NuGet for JSON serialization/deserialization

public partial class RabbitMQListener : Node
{
	private ConnectionFactory factory = new ConnectionFactory();
	private IConnection connection;
	private IModel channel;

	private string exchangeName = "PropagationProject";
	private string ROUTING_KEY_PROPAGATION = "propagation.affected.buildings"; // New routing key for propagation
	private string ROUTING_KEY_CLICK = "propagation.clicked.buidling"; // New routing key for click
	private string localQueue;
	private List<string> messages = new();

	private string userName = "incubator";
	private string hostName = "localhost";
	private string password = "incubator";
	private string port = "5672";

	[Signal]
	public delegate void OnMessageEventHandler(string message);

	public override void _Ready()
	{
		// Setup RabbitMQ connection details
		if (!string.IsNullOrEmpty(userName))
		{
			factory.UserName = userName;
			GD.Print("Host name set to: " + userName);
		}

		if (!string.IsNullOrEmpty(hostName))
		{
			GD.Print("Host name set to: " + hostName);
		}

		if (!string.IsNullOrEmpty(password))
		{
			factory.Password = password;
			GD.Print("Password set to: " + password);
		}

		if (!string.IsNullOrEmpty(port))
		{
			factory.Port = port.ToInt();
			GD.Print("Port set to: " + port);
		}
		else
		{
			factory.Port = 5672;
			GD.Print("Port not set, using default: 5672");
		}

		connection = factory.CreateConnection();
		channel = connection.CreateModel();

		// Declare queues and bind them to the appropriate routing keys
		localQueue = channel.QueueDeclare(autoDelete: true, exclusive: true);
		channel.QueueBind(queue: localQueue, exchange: exchangeName, routingKey: ROUTING_KEY_PROPAGATION);
		channel.QueueBind(queue: localQueue, exchange: exchangeName, routingKey: ROUTING_KEY_CLICK);

		ReceiveMessage();

		if (!connection.IsOpen)
		{
			GD.Print("Error! Could not connect!");
		}
		else
		{
			GD.Print("Connection established");
		}
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
		GD.Print("Waiting for messages...");
		var consumer = new EventingBasicConsumer(channel);

		consumer.Received += (model, ea) =>
		{
			var body = ea.Body.ToArray();
			var message = Encoding.ASCII.GetString(body);

			// If the message is from 'building.propagation', it will be a list of integers
			if (ea.RoutingKey == ROUTING_KEY_PROPAGATION)
			{
				List<int> propagationData = JsonConvert.DeserializeObject<List<int>>(message);
				GD.Print("Received propagation data: " + string.Join(", ", propagationData));
				messages.Add(message);
			}
		};

		channel.BasicConsume(queue: localQueue, autoAck: true, consumer: consumer);
	}

	private void Publish(int clickValue)
	{
		if (channel == null || !connection.IsOpen)
		{
			GD.PrintErr("❌ RabbitMQ connection is closed! Cannot publish.");
			return;
		}

		// Prepare the message as a single integer (for propagation.clicked.buidling)
		var body = Encoding.UTF8.GetBytes(clickValue.ToString());
		channel.BasicPublish(exchange: exchangeName, routingKey: ROUTING_KEY_CLICK, basicProperties: null, body: body);
		GD.Print("📩 Sent message to RabbitMQ (propagation.clicked.buidling): " + clickValue);
	}
}
