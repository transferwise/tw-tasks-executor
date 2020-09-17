package com.transferwise.tasks.demoapp.dbperftest;

import com.transferwise.tasks.ITasksService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class DbPerfTestService {

  @Autowired
  private ITasksService tasksService;

  public void addTask(int depth) {
    // Real world verification data for realistic compression testing.
    String data = "{\"eventUUID\":\"532479a0-1c8d-475a-8dd8-a9e0c85bed42\",\"heartBeatRequest\":{\"checkId\":19228887,\"profileId\":15658692," 
        + "\"actionToBeVerifiedEvent\":{\"eventType\":\"MONEY_MOVEMENT\",\"profileId\":15658692," 
        + "\"eventUUID\":\"122afd7a-3fd5-4aae-b393-9a8d698c4ee1\",\"eventCreationTime\":\"2020-08-28T20:33:14.827923\"," 
        + "\"actionType\":\"TRANSFER_CREATION\",\"invoiceValue\":35000.0,\"targetValue\":null,\"sourceCurrency\":\"JPY\"," 
        + "\"targetCurrency\":\"THB\",\"beneficiaryId\":\"110371303\",\"shouldRunAutoVerification\":true,\"moneyMovementType\":\"TRANSFER\"," 
        + "\"referenceId\":\"156944066\"},\"evidenceRequestDTOS\":[{\"id\":77569040,\"ruleId\":24862369,\"profileId\":15658692," 
        + "\"evidenceType\":\"JPN_SELFIE_WITH_LIVENESS_CHALLENGE\",\"evidenceState\":\"NOT_REQUIRED\",\"createdAt\":\"2020-07-23T14:17:07.58896\"," 
        + "\"updatedAt\":null},{\"id\":77569033,\"ruleId\":24862368,\"profileId\":15658692,\"evidenceType\":\"JPN_SELFIE_WITH_LIVENESS_CHALLENGE\"," 
        + "\"evidenceState\":\"NOT_VERIFIED\",\"createdAt\":\"2020-07-23T14:17:07.586101\",\"updatedAt\":null},{\"id\":77569037," 
        + "\"ruleId\":24862369,\"profileId\":15658692,\"evidenceType\":\"JPN_ID_DOC\",\"evidenceState\":\"NOT_REQUIRED\"," 
        + "\"createdAt\":\"2020-07-23T14:17:07.588961\",\"updatedAt\":null},{\"id\":77569038,\"ruleId\":24862369,\"profileId\":15658692," 
        + "\"evidenceType\":\"JPN_ID_DOC_WITH_LIVENESS_CHALLENGE\",\"evidenceState\":\"NOT_REQUIRED\",\"createdAt\":\"2020-07-23T14:17:07.588958\"," 
        + "\"updatedAt\":null},{\"id\":77569043,\"ruleId\":24862369,\"profileId\":15658692,\"evidenceType\":\"SNAIL_MAIL\"," 
        + "\"evidenceState\":\"NOT_REQUIRED\",\"createdAt\":\"2020-07-23T14:17:07.588956\",\"updatedAt\":null},{\"id\":77569035," 
        + "\"ruleId\":24862368,\"profileId\":15658692,\"evidenceType\":\"JPN_PERSONAL_MYNUMBER\",\"evidenceState\":\"NOT_VERIFIED\"," 
        + "\"createdAt\":\"2020-07-23T14:17:07.586102\",\"updatedAt\":null},{\"id\":77569031,\"ruleId\":24862368,\"profileId\":15658692," 
        + "\"evidenceType\":\"JPN_ID_DOC_WITH_LIVENESS_CHALLENGE\",\"evidenceState\":\"NOT_VERIFIED\",\"createdAt\":\"2020-07-23T14:17:07.586099\"," 
        + "\"updatedAt\":null},{\"id\":77569042,\"ruleId\":24862369,\"profileId\":15658692,\"evidenceType\":\"JPN_PERSONAL_MYNUMBER\"," 
        + "\"evidenceState\":\"NOT_REQUIRED\",\"createdAt\":\"2020-07-23T14:17:07.588962\",\"updatedAt\":null},{\"id\":77569029," 
        + "\"ruleId\":24862368,\"profileId\":15658692,\"evidenceType\":\"JPN_ID_DOC\",\"evidenceState\":\"NOT_SUITABLE\"," 
        + "\"createdAt\":\"2020-07-23T14:17:07.586101\",\"updatedAt\":null},{\"id\":77569030,\"ruleId\":24862368,\"profileId\":15658692," 
        + "\"evidenceType\":\"SNAIL_MAIL_FOR_FUTURE\",\"evidenceState\":\"NOT_SUITABLE\",\"createdAt\":\"2020-07-23T14:17:07.586103\"," 
        + "\"updatedAt\":null}]}}";
    
    
    tasksService.addTask(new ITasksService.AddTaskRequest().setDataString(data)
        .setType(DbPerfTestHandlerConfiguration.TASK_TYPE).setSubType(String.valueOf(depth)));
  }
}
