package es.unizar.tmdad.lab2.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.Tweet;

import es.unizar.tmdad.lab2.domain.MyTweet;
import es.unizar.tmdad.lab2.domain.TargetedTweet;
import es.unizar.tmdad.lab2.service.TwitterLookupService;

@Configuration
@EnableIntegration
@IntegrationComponentScan
@ComponentScan
public class TwitterFlow {
	
	private TwitterLookupService lookupService;
	
	@Bean
	public DirectChannel requestChannel() {
		return new DirectChannel();
	}

	// Tercer paso
	// Los mensajes se leen de "requestChannel" y se envian al método "sendTweet" del
	// componente "streamSendingService"
	@Bean
	public IntegrationFlow sendTweet() {
        // CAMBIOS A REALIZAR:
        //
        // Usando Spring Integration DSL
        //
        // Filter --> asegurarnos que el mensaje es un Tweet
        // Transform --> convertir un Tweet en un TargetedTweet con tantos tópicos como coincida
        // Split --> dividir un TargetedTweet con muchos tópicos en tantos TargetedTweet como tópicos haya
        // Transform --> señalar el contenido de un TargetedTweet
        //
		return IntegrationFlows.from(requestChannel())
				.filter(t -> t instanceof Tweet)
				.<Tweet,TargetedTweet>transform(t -> {
					MyTweet myTweet = new MyTweet(t);
					List<String> targets = lookupService.getQueries().stream()
											.filter(q -> t.getText().contains(q)).collect(Collectors.toList());
					return new TargetedTweet(myTweet, targets);
				})
				.split(TargetedTweet.class, t ->{
					List<TargetedTweet> tweets = new ArrayList<>();
					t.getTargets().forEach(q -> {
						tweets.add(new TargetedTweet(t.getTweet(),q));
					});
					return tweets;
				})
				.<TargetedTweet,TargetedTweet>transform(t->{
					t.getTweet().setUnmodifiedText(t.getTweet().getUnmodifiedText().replace(t.getFirstTarget(), "<b>"+t.getFirstTarget()+"</b>"));
					return t;
				})
				.handle("streamSendingService", "sendTweet").get();
		//Todos los mensajes que estén en RequestChannel, gestiónalos... 

	}

}

// Segundo paso
// Los mensajes recibidos por este @MessagingGateway se dejan en el canal "requestChannel"
@MessagingGateway(name = "integrationStreamListener", defaultRequestChannel = "requestChannel")
interface MyStreamListener extends StreamListener {

}
