#pragma once
#include <string>

namespace snb
{
    const std::string personPathSuffix="/dynamic/person_0_0.csv";
    const std::string placePathSuffix="/static/place_0_0.csv";
    const std::string orgPathSuffix="/static/organisation_0_0.csv";
    const std::string postPathSuffix="/dynamic/post_0_0.csv";
    const std::string commentPathSuffix="/dynamic/comment_0_0.csv";
    const std::string tagPathSuffix="/static/tag_0_0.csv";
    const std::string tagclassPathSuffix="/static/tagclass_0_0.csv";
    const std::string forumPathSuffix="/dynamic/forum_0_0.csv";
    
    const std::string personHasInterestPathSuffix="/dynamic/person_hasInterest_tag_0_0.csv";
    const std::string personKnowsPersonPathSuffix="/dynamic/person_knows_person_0_0.csv";
    const std::string personLikePostPathSuffix="/dynamic/person_likes_post_0_0.csv";
    const std::string personLikeCommentPathSuffix="/dynamic/person_likes_comment_0_0.csv";
    const std::string personStudyAtOrgPathSuffix="/dynamic/person_studyAt_organisation_0_0.csv";
    const std::string personWorkAtOrgPathSuffix="/dynamic/person_workAt_organisation_0_0.csv";
    const std::string postHasTagPathSuffix="/dynamic/post_hasTag_tag_0_0.csv";
    const std::string postHasCreatorPathSuffix="/dynamic/post_hasCreator_person_0_0.csv";
    const std::string postIsLocatedInPathSuffix="/dynamic/post_isLocatedIn_place_0_0.csv";
    const std::string commentHasTagPathSuffix="/dynamic/comment_hasTag_tag_0_0.csv";
    const std::string commentHasCreatorPathSuffix="/dynamic/comment_hasCreator_person_0_0.csv";
    const std::string forumHasTagPathSuffix="/dynamic/forum_hasTag_tag_0_0.csv";
    const std::string forumHasMemberPathSuffix="/dynamic/forum_hasMember_person_0_0.csv";
    const std::string forumHasModeratorPathSuffix="/dynamic/forum_hasModerator_person_0_0.csv";
    const std::string forumContainPostPathSuffix="/dynamic/forum_containerOf_post_0_0.csv";
    const std::string tagHasTypePathSuffix="/static/tag_hasType_tagclass_0_0.csv";
    const std::string placeIsPartOfPathSuffix="/static/place_isPartOf_place_0_0.csv";
    const std::string personIsLocatedInPathSuffix="/dynamic/person_isLocatedIn_place_0_0.csv";
    const std::string orgIsLocatedInPathSuffix="/static/organisation_isLocatedIn_place_0_0.csv";
    const std::string tagClassIsSubclassOfPathSuffix="/static/tagclass_isSubclassOf_tagclass_0_0.csv";
    const std::string commentReplyOfCommentPathSuffix="/dynamic/comment_replyOf_comment_0_0.csv";
    const std::string commentReplyOfPostPathSuffix="/dynamic/comment_replyOf_post_0_0.csv";
    const std::string commentIsLocatedInPathSuffix="/dynamic/comment_isLocatedIn_place_0_0.csv";
}
