import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by das on 8/18/19.
 */
public class RandomLinkGenearator
{
    private static final int BASE_DURATION_IN_MILLIS = 1000;

    private static final int NUM_INSTANCE_PER_RUN = 100;

    private static final int NUM_USERS = (int)10e6;

    private static final int NUM_BASE_LINKS = (int)10e6;

    private static final int NUM_DAILY_LINKS = (int)10e4;

    private static final int BASE_LINK_PCT_WM = 40;

    private static final int LO_USER_REUSE_PCT_WM = 13;
    private static final int HI_USER_REUSE_PCT_WM = 37;
    private static final int LO_LINK_REUSE_PCT_WM = 22;
    private static final int HI_LINK_REUSE_PCT_WM = 67;

    public static void main (String[] args) {
        generate();
    }

    private static void generate() {
        try {
            long stTime = System.currentTimeMillis();

            Random rand = new Random();

            List<String> currentRunUserCache = new LinkedList<String>();
            List<String> currentRunLinkCache = new LinkedList<String>();

            // decide percentage of users that will be shared during one run
            int userReusePct = rand.nextInt((HI_USER_REUSE_PCT_WM - LO_USER_REUSE_PCT_WM) + 1) + LO_USER_REUSE_PCT_WM;

            // decide percentage of links that will be shared during one run
            int linkReusePct = rand.nextInt((HI_LINK_REUSE_PCT_WM - LO_LINK_REUSE_PCT_WM) + 1) + LO_LINK_REUSE_PCT_WM;

            for (int i = 0; i < NUM_INSTANCE_PER_RUN; i++) {
                String instanceUser = "U_", instanceLink = "L_";

                // generate a new user or reuse
                int reuseUserCount = (userReusePct * NUM_INSTANCE_PER_RUN) / 100;
                if (i < NUM_INSTANCE_PER_RUN - reuseUserCount) { // new user -- random
                    String newUser = String.valueOf(rand.nextInt(NUM_USERS));
                    currentRunUserCache.add(newUser);
                    instanceUser += newUser;
                } else { // reuse
                    int randUserInd = rand.nextInt(currentRunUserCache.size());
                    instanceUser += currentRunUserCache.get(randUserInd);
                }

                // generate a new link or reuse
                int reuseLinkCount = (linkReusePct * NUM_INSTANCE_PER_RUN) / 100;
                if (i < NUM_INSTANCE_PER_RUN - reuseLinkCount) { // new link -- randomly determines from base or today
                    int randBaseMark = rand.nextInt(100);
                    String newLink = randBaseMark < BASE_LINK_PCT_WM ?
                        String.valueOf(rand.nextInt(NUM_BASE_LINKS)) :
                        String.valueOf(rand.nextInt(NUM_DAILY_LINKS)) + "_" + new Date().toString().replace(" ", "_");
                    currentRunLinkCache.add(newLink);
                    instanceLink += newLink;
                } else { // reuse
                    int randLinkInd = rand.nextInt(currentRunLinkCache.size());
                    instanceLink += currentRunLinkCache.get(randLinkInd);
                }

                print(instanceUser + " -- " + instanceLink);

                // sleep for a random think time
                Thread.sleep(5);
            }

            long duration = System.currentTimeMillis() - stTime;
            print("Took " + duration + " milliseconds to complete ");

            print("USER REUSE% " + userReusePct);
            print("reused " + (NUM_INSTANCE_PER_RUN - currentRunUserCache.size()));

            print("LINK REUSE% " + linkReusePct);
            print("reused " + (NUM_INSTANCE_PER_RUN - currentRunLinkCache.size()));
            if (duration < BASE_DURATION_IN_MILLIS) {
                print("sleeping " + (BASE_DURATION_IN_MILLIS - duration) + " milliseconds...");
                Thread.sleep(BASE_DURATION_IN_MILLIS - duration);
            }
        } catch (Exception e) {
            System.out.println("System error: " + e);
        }
    }

    private static void print(String s) {
        System.out.println(s);
    }
}
