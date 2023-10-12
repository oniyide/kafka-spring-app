package dev.oniyide.integration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.client.WireMock;


public class WiremockUtils
{
    public static void reset(){
        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();
    }

    public static void stubWiremock(final String url, final int httpStatusResponse, final String body){
        stubWiremock(url, httpStatusResponse, body, null, null, null);
    }

    public static void stubWiremock(final String url,
                                    final int httpStatusResponse,
                                    final String body,
                                    final String scenario,
                                    final String initialState,
                                    final String nextState){
        if(scenario != null){
            stubFor(get(urlEqualTo(url))
                .inScenario(scenario)
                .whenScenarioStateIs(initialState)
                .willReturn(aResponse().withStatus(httpStatusResponse).withHeader("Content-Type", "text/plain").withBody(body))
                .willSetStateTo(nextState));
        } else {
            stubFor(get(urlEqualTo(url))
                .willReturn(aResponse().withStatus(httpStatusResponse).withHeader("Content-Type", "text/plain").withBody(body)));
        }
    }

}
