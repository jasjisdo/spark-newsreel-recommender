package de.dailab.newsreel.recommender.common.extimpl.unit.extimpl;

import de.dailab.newsreel.recommender.common.item.IdomaarItem;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by domann on 18.12.15.
 */
public class IdomaarItemTest {

    String request;

    @Before
    public void setUp() throws Exception {
			request = getRequest();
    }

    @Test
    public void whenIdomaaritemIsInitialized_ThenNoExceptionsAndAllRequieredFieldsAreCorrect() throws Exception {

        // prepare request
        String[] splittedRequest = request.split("\\t");
        String propertyValue = splittedRequest[3];
        String entityValue = splittedRequest[4];

        // create Idomaar item with implicit json parser for fields.
        IdomaarItem item = new IdomaarItem(propertyValue, entityValue);

        // check values of requiered fields.
        assertThat(item.getItemID(), is(186312774));
        assertThat(item.getUserID(), is(71131568));
        assertThat(item.getDomainId(), is(1677L));
    }

    @Ignore("changed structure of idomaar item")
	@Test
	public void uidIsZero() throws Exception {

		// prepare request
		request = getRequest(0, 186312774, 1677L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);

		// check if uid is 0
		assertThat(item.getUserID(), is(0));
		assertThat(item.getDomainId(), is(1677L));
		assertThat(item.getItemID(), is(186312774));
	}

    @Ignore("changed structure of idomaar item")
	@Test
	public void uidIsWeird() throws Exception {

		// prepare request
		request = getRequest(1234567890, 186312774, 1677L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);

		// check if uid is 0
		assertThat(item.getUserID(), is(0));
		assertThat(item.getDomainId(), is(1677L));
		assertThat(item.getItemID(), is(186312774));
	}

    @Ignore("changed structure of idomaar item")
	@Test
	public void uidIsBelowZero() throws Exception {

		// prepare request
		request = getRequest(-1, 186312774, 1677L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);

		// check if uid is 0
		assertThat(item.getUserID(), is(0));
		assertThat(item.getDomainId(), is(1677L));
		assertThat(item.getItemID(), is(186312774));
	}

	@Test(expected = IllegalArgumentException.class)
	public void iidIsZero() throws Exception {

		// prepare request
		request = getRequest(0, 1677L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);
	}

	@Test(expected = IllegalArgumentException.class)
	public void didIsZero() throws Exception {

		// prepare request
		request = getRequest(0L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);
	}

	@Test(expected = IllegalArgumentException.class)
	public void iidAndDidAreZero() throws Exception {

		// prepare request
		request = getRequest(0, 0L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);
	}

	@Test(expected = IllegalArgumentException.class)
	public void allArgsZero() throws Exception {

		// prepare request
		request = getRequest(0, 0, 0L);
		String[] splittedRequest = request.split("\\t");
		String propertyValue = splittedRequest[3];
		String entityValue = splittedRequest[4];
		
		// create idomaar item
		IdomaarItem item = new IdomaarItem(propertyValue, entityValue);
	}


	
	private String getRequest(int uid, int iid, long did) {
		return "recommendation\t9125952001677\t1404165599401\t{\"recs\": {\"ints\": {\"3\": [186337307, " +
                "183665084, 186097159, 186236234, 180818566, 186374324]}}, \"event_type\": \"recommendation_request\", " +
                "\"context\": {\"simple\": {\"62\": 1979832, \"63\": 1840689, \"49\": 48, \"67\": 1928642, \"68\": " +
                "1851453, \"69\": 1851422, \"24\": 2, \"25\": " + iid + ", \"27\": " + did + ", \"22\": 61970, \"23\": 23," +
                " \"47\": 654013, \"44\": 1851485, \"42\": 0, \"29\": 17332, \"40\": 1788350, \"41\": 25, \"5\": 280," +
                " \"4\": 40293, \"7\": 18873, \"6\": 952253, \"9\": 26889, \"13\": 2, \"76\": 1, \"75\": 1919968, \"74\":" +
                " 1919860, \"39\": 748, \"59\": 1275566, \"14\": 33331, \"17\": 48985, \"16\": 48811, \"19\": 52193," +
                " \"18\": 5, \"57\": " + uid + ", \"56\": 1138207, \"37\": 1978123, \"35\": 315003, \"52\": 1, \"31\": 0}," +
                " \"clusters\": {\"46\": {\"472375\": 100, \"472420\": 100, \"472376\": 100}, \"51\": {\"2\": 255}," +
                " \"1\": {\"7\": 255}, \"33\": {\"32962448\": 6, \"2559246\": 2, \"328109\": 8, \"10758\": 1," +
                " \"2033354\": 0, \"556907\": 7, \"236223\": 3, \"4146\": 5, \"39698\": 3, \"399975\": 10, " +
                "\"60664\": 2, \"32941223\": 5, \"7354\": 7, \"101707\": 1, \"2566836\": 1, \"51732\": 2, \"404677\":" +
                " 5}, \"3\": [55, 28, 34, 91, 23, 21], \"2\": [11, 11, 61, 60, 61, 26, 21], \"64\": {\"4\": 255}," +
                " \"65\": {\"1\": 255}, \"66\": {\"12\": 255}}, \"lists\": {\"11\": [13839], \"8\": [18841, 18842," +
                " 48511], \"10\": [6, 13, 1768, 1769, 1770]}}, \"timestamp\": 1404165599401}\t{\"userID\":71131568," +
                " \"itemID\":186312774, \"domainID\":1677}\t";
	}

	private String getRequest(int iid, long did) {
		return getRequest(71131568, iid, did);
	}

	private String getRequest(long did) {
		return getRequest(71131568, 186312774, did);
	}

	private String getRequest() {
		return getRequest(71131568, 186312774, 1677L);
	}
}
