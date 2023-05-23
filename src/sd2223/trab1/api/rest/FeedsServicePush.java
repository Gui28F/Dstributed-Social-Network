package sd2223.trab1.api.rest;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import sd2223.trab1.api.PushMessage;

public interface FeedsServicePush extends sd2223.trab1.api.rest.FeedsService {
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	void push_PushMessage(@HeaderParam(FeedsService.HEADER_VERSION) Long version, PushMessage msg);

	@PUT
	@Path("/followers/{" + USERSUB + "}/{" + USER + "}")
	@Consumes(MediaType.APPLICATION_JSON)
	void push_updateFollowers(@HeaderParam(FeedsService.HEADER_VERSION) Long version,@PathParam(USERSUB) String user, @PathParam(USER) String follower, boolean following);
}
