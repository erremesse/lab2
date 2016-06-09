package es.unizar.tmdad.lab2.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.social.twitter.api.FilterStreamParameters;
import org.springframework.social.twitter.api.Stream;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.impl.TwitterTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;

import es.unizar.tmdad.lab2.domain.TargetedTweet;

@Service
public class StreamSendingService {
	

	@Autowired
	private SimpMessageSendingOperations ops;
	
	@Autowired
	private TwitterTemplate twitterTemplate;

	@Autowired
	private TwitterLookupService lookupService;
	
	private Stream stream;

	@Autowired
	private StreamListener integrationStreamListener;

	@PostConstruct
	public void initialize() {
		FilterStreamParameters fsp = new FilterStreamParameters();
		fsp.addLocation(-180, -90, 180, 90);

		// Primer paso
		// Registro un gateway para recibir los mensajes (clase que va a actuar como receptora de mensajes para pasarlos a Spring integration)
		// Ver @MessagingGateway en MyStreamListener en TwitterFlow.java
		stream = twitterTemplate.streamingOperations().filter(fsp, Collections.singletonList(integrationStreamListener)); 
		//IntegrationStreamListener es un bean. Spring Integration exige que sea una interfaz (cogemos el Stream de Twitter)
		//... cualquier invocación a cualquier método lo convierto en un mensaje de entrada en el canal...
	}

	// Cuarto paso
	// Recibe un tweet y hay que enviarlo a tantos canales como preguntas hay registradas en lookupService
	//
	public void sendTweet(Tweet tweet) {
		Map<String, Object> map = new HashMap<>();
		map.put(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON);

		//NOTA>> Ver código original sin lambdas (transparencias)
			//Código imperativo: dice lo que hay que hacer
			//Código funcional: Uso de streams (cualquier lista se pasa "automáticamente" a stream.
				//A estos streams se les puede aplicar filtros y producción(foreach)
				//
		// Expresión lambda: si el tweet contiene s, devuelve true //Predicate espera que se devuelva Boolean
		Predicate<String> notContainsTopic = s -> tweet.getText().contains(s); //Why "notContainsTopic"??? -> "containsTopic"
		// Expresión lambda: envia un tweet al canal asociado al tópico s // Consumer espera Void
		Consumer<String> convertAndSend = s -> ops.convertAndSend("/queue/search/" + s, tweet, map);

		// notContainsTopic & convertAndSend -> funciones/expresiones lambda (sería como una versión mejorada de clases anónimas)
		lookupService.getQueries().stream().filter(notContainsTopic).forEach(convertAndSend);
	}

	public void sendTweet(TargetedTweet targetedTweet) {
		// CAMBIOS A REALIZAR:
		//
		// Crea un mensaje que envie un tweet a un único tópico destinatario
		//
		Map<String, Object> map = new HashMap<>();
		map.put(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON);
		ops.convertAndSend("/queue/search/" + targetedTweet.getFirstTarget(),
				targetedTweet.getTweet(),map);

	}


	public Stream getStream() {
		return stream;
	}

}
